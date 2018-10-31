# -*- coding: utf-8 -*-
# Copyright 2018 Etsy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import functools
from six.moves import filter
import networkx as nx
from boundary_layer.logger import logger
from boundary_layer.exceptions import CyclicWorkflowException, InvalidFlowControlNode
from boundary_layer import util


class _GraphUtil(object):
    @staticmethod
    def get_upstream_surface(graph):
        return [node for node in graph.nodes()
                if not list(graph.predecessors(node))]

    @staticmethod
    def get_downstream_surface(graph):
        return [node for node in graph.nodes()
                if not list(graph.successors(node))]

    @staticmethod
    def upstream_dependency_set(node, graph):
        return frozenset(graph.predecessors(node))

    @staticmethod
    def downstream_dependency_set(node, graph):
        return frozenset(graph.successors(node))

    @staticmethod
    def leaf_nodes(graph):
        return [node for node in graph.nodes()
                if not list(graph.successors(node))]

    @staticmethod
    def get_distinct_upstream_dependency_sets(nodes, graph):
        dependency_sets = [
            _GraphUtil.upstream_dependency_set(node, graph)
            for node in nodes]

        return frozenset(filter(None, dependency_sets))

    @staticmethod
    def build_subgraph(nodes):
        graph = nx.DiGraph()

        node_lookup = {node.name: node for node in nodes}

        # First insert every node into the graph
        graph.add_nodes_from(nodes)

        # Now insert every explicit edge into the graph, making sure that
        # any referenced node is actually present in the subgraph
        for node in nodes:
            logger.debug('Processing node %s: upstream %s downstream %s',
                         node, node.upstream_dependencies, node.downstream_dependencies)
            for upstream in node.upstream_dependencies:
                upstream_node = node_lookup.get(upstream)
                if not upstream_node:
                    raise Exception('Invalid reference {} <- {}: perhaps it '
                                    'crosses sub-graph boundaries?'.format(
                                        upstream,
                                        node.name))

                graph.add_edge(upstream_node, node)

            for downstream in node.downstream_dependencies:
                downstream_node = node_lookup.get(downstream)

                if not downstream_node:
                    raise Exception('Invalid reference {} -> {}: perhaps it '
                                    'crosses sub-graph boundaries?'.format(
                                        node.name,
                                        downstream))

                graph.add_edge(node, downstream_node)

        if not nx.algorithms.dag.is_directed_acyclic_graph(graph):
            raise CyclicWorkflowException('Invalid sub-graph: not a DAG!')

        return graph

    @staticmethod
    def ordered(graph):
        return list(nx.algorithms.dag.topological_sort(graph))

    @staticmethod
    def attach_flow_control_between(
            upstream,
            downstream,
            graph,
            fc_node=None,
            fc_node_builder=None):
        """ For each downstream node, break that node's connections with all of
            the upstream nodes provided, and insert a flow-control node between
            them.  If the fc_node argument is provided, then use the provided node
            for flow-control; otherwise, insert a new flow-control node.
        """
        if not upstream or not downstream:
            raise Exception('upstream and downstream node lists cannot be '
                            'empty (given upstream `{}`, downstream `{}`)'.format(
                                upstream, downstream))

        if not fc_node and not fc_node_builder:
            raise Exception(
                'Internal error: neither fc_node nor fc_node_builder argument '
                'was provided to the method attach_flow_control_between() '
                'but exactly one of these is required')

        if fc_node and fc_node_builder:
            raise Exception(
                'Internal error: both fc_node and fc_node_builder arguments '
                'were provided to the method attach_flow_control_between() '
                'but exactly one of these is required')

        if not fc_node:
            fc_node = fc_node_builder(deps=downstream)
            logger.debug('Inserting new flow control node %s between '
                         '{ %s } and { %s }', fc_node, upstream, downstream)

            graph.add_node(fc_node)
        else:
            logger.debug('Attaching existing node %s between '
                         '{ %s } and { %s }', fc_node, upstream, downstream)

        for node in downstream:
            if not upstream.issubset(_GraphUtil.upstream_dependency_set(node, graph)):
                raise InvalidFlowControlNode(
                    'Internal error: Invalid flow-control insertion. '
                    'Upstream nodes {} are not all upstream dependencies of '
                    'node {}'.format(upstream, node))

        # Break the edges between the upstream and downstream nodes
        break_edges = [(up, down) for up in upstream for down in downstream]
        logger.debug('removing edges: %s', break_edges)
        graph.remove_edges_from(break_edges)

        # Now insert the flow control node between the upstream and downstream
        # dependencies
        add_edges = [(up, fc_node) for up in upstream] + \
                    [(fc_node, down) for down in downstream]

        logger.debug('adding edges: %s', add_edges)
        graph.add_edges_from(add_edges)

    @staticmethod
    def upstream_resource_boundary(resource_name, graph):
        return [node for node in graph.nodes()
                if resource_name in node.requires_resources and
                all(resource_name not in ancestor.requires_resources
                    for ancestor in nx.algorithms.dag.ancestors(graph, node))]

    @staticmethod
    def downstream_resource_boundary(resource_name, graph):
        """ Determine the downstream boundary nodes for the provided resource.
            This is the set of nodes N where, for any n in N, n requires the
            resource, but there do not exist any m, downstream of n, such that
            m also requires the resouce

            :param resource_name: name of the resource
            :type resource_name: string
            :param graph: the graph
            :type graph: nx.DiGraph
            :return The downstream boundary for the provided resource
            :rtype list<boundary_layer.registry.RegistryNode>
        """
        return [node for node in graph.nodes()
                if resource_name in node.requires_resources and
                all(resource_name not in descendant.requires_resources
                    for descendant in nx.algorithms.dag.descendants(graph, node))]

    @staticmethod
    def requires_destroy_resource_sentinel(downstream_resource_boundary, graph):
        """ Determine whether the destroy-resource operator created downstream
            of the specified resource will mask any upstream task failures, and
            therefore whether we should insert a sentinel node in parallel with
            the destroy-resource node.  This is true if any of the downstream
            resource boundary nodes are leaf nodes of the graph, ie nodes that
            have no downstream nodes to which to propagate error statuses.

            :param downstream_resource_boundary: downstream resource boundary
            :type downstream_resource_boundary: list<boundary_layer.registry.RegistryNode>
            :param graph: the graph
            :type graph: nx.DiGraph
        """
        return bool(
            frozenset(_GraphUtil.leaf_nodes(graph)) &
            frozenset(downstream_resource_boundary))

    @staticmethod
    def insert_bundler_nodes(nodes, graph, fc_node_builder, default_bundler_node=None):
        """ Utility for inserting "bundler" nodes that help us express
            complex dependency settings that are not otherwise supported by
            Airflow.  In particular: suppose we have a node, N, that we would
            like to trigger whenever either (A and B) are complete or
            (C and D) are complete.  Airflow does not have any mechanism for
            expressing this directly.  Instead, we insert a bundler node X0
            that triggers when (A and B) are complete, and a bundler node X1
            that triggers when (C and D) are complete, and then we set Z to
            trigger when either X0 or X1 is complete, using the ONE_SUCCESS
            trigger rule.

            The specific use case for this logic in boundary-layer is for
            creating shared resources when any resource-dependent operator is
            ready, based on its upstream dependencies being satisfied, even if
            the upstream dependencies of other resource-dependent operators
            may not yet be satisfied.

            :param bundles: list of sets of nodes
            :param graph: graph
            :param default_bundler_node: preexisting node to use as the bundler
                if there is ony one bundle in the input.  This parameter is
                ignored if there are more than one bundle; in that case, or if
                this parameter is not provided at all, new flow control nodes
                will be inserted to act as bundler nodes.
        """

        bundles = _GraphUtil.get_distinct_upstream_dependency_sets(
            nodes, graph)

        bundler_nodes = _GraphUtil._create_bundler_nodes(
            bundles,
            fc_node_builder,
            default_bundler_node,
            )

        to_insert = [node for node in bundler_nodes.values() if node !=
                     default_bundler_node]
        logger.debug('Inserting bundler nodes: %s', to_insert)
        graph.add_nodes_from(to_insert)

        for node in nodes:
            dep_set = _GraphUtil.upstream_dependency_set(node, graph)

            if not dep_set:
                # Some nodes may not have had any upstream dependencies, and
                # no bundler nodes would have been added for these
                continue

            bundler_node = bundler_nodes[dep_set]

            _GraphUtil.attach_flow_control_between(
                upstream=dep_set,
                downstream=frozenset([node]),
                graph=graph,
                fc_node=bundler_node)

        return bundler_nodes

    @staticmethod
    def _create_bundler_nodes(bundles, fc_node_builder, default_bundler_node=None):
        if len(bundles) == 1 and default_bundler_node:
            return {
                list(bundles)[0]: default_bundler_node,
            }

        # If there is more than one way in which the boundary nodes
        # depend on upstream nodes, then we create flow-control
        # nodes that allow us to trigger the resource creation
        # whenever the first boundary node would have otherwise
        # been available (even if others still have unmet dependencies).
        # Note that we may opt not to do this, and instead trigger only
        # after all of the boundary nodes would have been ready, but that
        # gets pushed to a higher level configuration.
        return {
            deps: fc_node_builder(deps=deps)
            for deps in bundles
        }

    @staticmethod
    def attach_create_resource(resource, graph, fc_node_builder):
        create_resource_node = resource.create_operator
        graph.add_node(create_resource_node)

        upstream_boundary = \
            _GraphUtil.upstream_resource_boundary(resource.name, graph)

        logger.debug('upstream boundary nodes for resource `%s`: %s',
                     resource.name, upstream_boundary)

        bundler_nodes = _GraphUtil.insert_bundler_nodes(
            upstream_boundary,
            graph,
            fc_node_builder,
            create_resource_node)

        for bundler_node in bundler_nodes.values():
            if bundler_node == create_resource_node:
                continue

            graph.add_edge(bundler_node, create_resource_node)

        for node in upstream_boundary:
            dep_set = _GraphUtil.upstream_dependency_set(node, graph)
            assert len(dep_set) <= 1, \
                'Error: node {} has dependency set {}, but its ' \
                'dependencies should have been bundled!'.format(
                    node,
                    dep_set)

            bundler_node = list(dep_set)[0] if dep_set else None

            # If the create_resource_node is functioning as the bundler
            # node, then all necessary dependencies will have been set up
            # already.
            if bundler_node == create_resource_node:
                continue

            # Otherwise, we have to insert dependencies between the
            # create_resource_node and the downstream node.
            if not bundler_node:
                logger.debug(
                    'Adding edge between create_resource_node and dependency-free node %s',
                    node)
            else:
                logger.debug(
                    'Adding edge between create_resource_node and node %s with bundler node %s',
                    node,
                    bundler_node)

            graph.add_edge(create_resource_node, node)

    @staticmethod
    def attach_destroy_resource(resource, graph, fc_node_builder):
        """ Method to attach the destroy-resource node to the graph,
            which is simpler than the attachment of the create-resource node
            because it can be attached directly to the downstream boundary nodes
            without breaking any existing dependencies.
        """
        destroy_resource = resource.destroy_operator
        if not destroy_resource:
            return

        downstream_boundary = _GraphUtil.downstream_resource_boundary(
            resource.name,
            graph)

        logger.debug('downstream boundary nodes for resource `%s`: %s',
                     resource.name, downstream_boundary)

        nodes_to_add = [destroy_resource]

        if _GraphUtil.requires_destroy_resource_sentinel(downstream_boundary, graph):
            # The destroy_resource node is the only leaf node in the DAG.
            # Add a flow control node to the graph that will inherit its state
            # from the nodes that are upstream to the destroy_resource node.
            # This is because Airflow's DagRun final-state logic only looks
            # at leaf node states, and if the destroy_resource step runs
            # successfully after an upstream failure then the DagRun would
            # be marked as a success, which is presumably not the desired
            # behavior.

            if resource.disable_sentinel_node:
                logger.debug(
                    'Not adding a sentinel node for resource %s because '
                    'the DAG or the resource configuration specified '
                    'disable_sentinel_node == True',
                    destroy_resource.name)
            else:
                logger.debug(
                    '`%s` is the only leaf node in the DAG: adding a '
                    'sentinel node to propagate upstream failures to '
                    'the DagRun state',
                    destroy_resource.name)
                fc_node = fc_node_builder(
                    deps=downstream_boundary,
                    name=destroy_resource.name + '-sentinel')
                nodes_to_add.append(fc_node)

        graph.add_nodes_from(nodes_to_add)

        for node in downstream_boundary:
            logger.debug(
                'Adding edge between node %s and %s', node, nodes_to_add)
            graph.add_edges_from((node, new_node) for new_node in nodes_to_add)

    @staticmethod
    def prune_node(node, graph):
        if node not in graph:
            raise Exception('Cannot prune node {}: not present in graph {}'.format(
                node, graph))

        upstream = list(graph.predecessors(node))
        downstream = list(graph.successors(node))

        logger.debug('Pruning node %s with upstream nodes %s and downstream nodes %s',
                     node, upstream, downstream)

        graph.add_edges_from((up, down)
                             for up in upstream for down in downstream)

        graph.remove_node(node)

        return graph

    @staticmethod
    def prune_nodes(graph, nodes=None, node_selector=None):
        if not nodes and not node_selector:
            return graph

        components_before_prune = list(
            nx.algorithms.components.weakly_connected_components(graph))

        prune_nodes = nodes if nodes else \
            [node for node in graph.nodes if node_selector(node)]

        for node in prune_nodes:
            _GraphUtil.prune_node(node, graph)

        components_after_prune = list(
            nx.algorithms.components.weakly_connected_components(graph))

        if len(components_before_prune) < len(components_after_prune):
            logger.warning('Pruning of nodes %s from graph %s caused components to '
                           'become disconnected! Components before prune: %s; after prune: %s',
                           prune_nodes,
                           graph,
                           components_before_prune,
                           components_after_prune)

        return graph


class OperatorGraph(object):
    def get_upstream_surface(self):
        return _GraphUtil.get_upstream_surface(self.graph)

    def get_downstream_surface(self):
        return _GraphUtil.get_downstream_surface(self.graph)

    def upstream_dependency_set(self, node):
        return _GraphUtil.upstream_dependency_set(node, self.graph)

    def downstream_dependency_set(self, node):
        return _GraphUtil.downstream_dependency_set(node, self.graph)

    def ordered(self):
        return _GraphUtil.ordered(self.graph)

    def __init__(self, dag, default_task_args=None):
        self.dag = dag

        self.fc_node_builder = functools.partial(
            util.make_flow_control_node,
            default_task_args=default_task_args or {})

        main_graph = _GraphUtil.build_subgraph(
            dag.get('operators', []) +
            dag.get('sub_dags', []) +
            dag.get('generators', []))

        before_graph = _GraphUtil.build_subgraph(dag.get('before', []))
        after_graph = _GraphUtil.build_subgraph(dag.get('after', []))

        resource_graph = _GraphUtil.build_subgraph(dag.get('resources', []))

        self.graph = nx.algorithms.operators.union_all([
            main_graph,
            before_graph,
            after_graph])

        self.nodes = {node.name: node for node in self.graph.nodes()}
        for upstream_node in _GraphUtil.get_downstream_surface(before_graph):
            for downstream_node in _GraphUtil.get_upstream_surface(main_graph):
                self.graph.add_edge(upstream_node, downstream_node)

        for downstream_node in _GraphUtil.get_upstream_surface(after_graph):
            for upstream_node in _GraphUtil.get_downstream_surface(main_graph):
                self.graph.add_edge(upstream_node, downstream_node)

        # Finally get all resources and figure out how to attach them to the
        # graph
        for resource in resource_graph:
            logger.debug('Attaching resource %s to graph', resource)
            _GraphUtil.attach_create_resource(resource, self.graph, self.fc_node_builder)
            _GraphUtil.attach_destroy_resource(resource, self.graph, self.fc_node_builder)

        self._attach_generator_flow_control()

    def _attach_generator_flow_control(self):
        generators = frozenset(self.dag.get('generators', []))

        for generator in generators:
            downstream_generators = generators & _GraphUtil.downstream_dependency_set(
                generator, self.graph)

            if not downstream_generators:
                continue

            _GraphUtil.attach_flow_control_between(
                upstream=frozenset([generator]),
                downstream=downstream_generators,
                graph=self.graph,
                fc_node_builder=self.fc_node_builder)
