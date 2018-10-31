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

import os
from collections import namedtuple, Counter

import six
import yaml

from boundary_layer.schemas.dag import PrimaryDagSchema, BaseDagSchema
from boundary_layer.graph import OperatorGraph
from boundary_layer.builders.base import DagBuilderBase
from boundary_layer.exceptions import InvalidWorkflowReference, InvalidNodeReference
from boundary_layer.graph import _GraphUtil
from boundary_layer.logger import logger
from boundary_layer.containers import WorkflowMetadata, ExecutionContext

from boundary_layer import plugins, pretty_yaml

ParsedSpec = namedtuple('ParsedSpec', ['primary', 'secondary'])
GraphSpec = namedtuple('GraphSpec', ['primary', 'secondary'])

DagSpec = namedtuple('DagSpec', ['parsed', 'graphs'])


class Workflow(object):
    @staticmethod
    def load(filename):
        with open(filename) as _input:
            content = _input.read()

        items = list(yaml.safe_load_all(content))

        if not items:
            raise Exception('No DAG found in file {}'.format(filename))

        if any(item is None for item in items):
            raise Exception('One or more empty DAGs found in file {}'.format(filename))

        return Workflow(
            primary=items[0],
            secondary=items[1:],
            filename=os.path.abspath(filename),
            yaml_config=content,
        )

    def __init__(self, primary, secondary, filename=None, yaml_config=None):
        self.filename = filename
        self.primary = primary
        self.secondary = secondary
        self._yaml_config = yaml_config

        self.specs = self._build_specs()

    @property
    def yaml_config(self):
        if self._yaml_config:
            return self._yaml_config

        return pretty_yaml.dump_all([self.primary] + list(self.secondary))

    def _build_specs(self):
        # The first step in validation is to parse the dags and let the schemas
        # set all defaults, check for bad values, etc.
        parsed_spec = ParsedSpec(
            primary=self.parse_dag(self.primary, PrimaryDagSchema),
            secondary=[self.parse_dag(dag, BaseDagSchema) for dag in self.secondary])

        # Now we do some collective checks on all of the dags at once.
        # First that each dag (primary plus secondary) has a unique name
        Workflow.validate_dag_names(parsed_spec)

        # Now make sure that all references are valid, and that all secondary
        # dags are connected to the primary dag.
        Workflow.validate_references(parsed_spec)

        # Now make sure that all requested resources exist, and that all
        # existing resources are requested.  Note that this does not check
        # to make sure that all resources will be available in the context of
        # each operator: for example, resources created in a sub-dag but
        # referenced in a primary dag will not be detected here, but will be
        # detected later at DAG-construction time.
        Workflow.validate_and_resolve_properties(parsed_spec)

        # Now build the graphs.  This does some additional checking, too.
        default_task_args = parsed_spec.primary.get('default_task_args', {})
        graph_spec = GraphSpec(
            primary=OperatorGraph(parsed_spec.primary, default_task_args),
            secondary=[OperatorGraph(dag, default_task_args) for dag in parsed_spec.secondary])

        return DagSpec(
            parsed=parsed_spec,
            graphs=graph_spec)

    @staticmethod
    def validate_dag_names(spec):
        id_counts = Counter(dag['name']
                            for dag in [spec.primary] + spec.secondary)

        dupes = [name for (name, count) in id_counts.items() if count > 1]

        if dupes:
            raise ValueError('Duplicate dag names: {}'.format(dupes))

    @staticmethod
    def validate_references(spec):
        """ Check the dag's references for validity: make sure that all
            references exist, that every secondary dag appears at least once
            as a target, and that all secondary dags are used either as a
            sub_dag or a generator, but not as both, because those would
            almost certainly be incompatible.
        """

        secondary_names = frozenset(dag['name'] for dag in spec.secondary)
        targets = Workflow.get_target_names(spec)

        if spec.primary['name'] in targets:
            raise ValueError('Illegal reference: primary dag `{}` cannot be '
                             'used as a sub-dag or generator target!'.format(
                                 spec.primary['name']))

        missing_targets = targets - secondary_names
        unused_targets = secondary_names - targets

        if missing_targets:
            raise ValueError('Bad references: sub-dags {} are missing!'.format(
                missing_targets))

        if unused_targets:
            raise ValueError('Unreachable sub-dag names found: {}'.format(
                unused_targets))

    @staticmethod
    def get_target_names(spec):
        all_dags = [spec.primary] + spec.secondary

        sub_dag_targets = frozenset([
            sub_dag.item['target'] for dag in all_dags
            for sub_dag in dag['sub_dags']])

        generator_targets = frozenset([
            generator.item['target'] for dag in all_dags
            for generator in dag['generators']])

        mixed_secondary_types = sub_dag_targets & generator_targets
        if mixed_secondary_types:
            raise ValueError('Invalid cross-listing of secondary DAGs as '
                             'both sub-dags and generators: {}'.format(
                                 mixed_secondary_types))

        return sub_dag_targets | generator_targets

    @staticmethod
    def validate_and_resolve_properties(spec):
        secondary_lookup = {dag['name']: dag for dag in spec.secondary}
        default_task_args = spec.primary.get('default_task_args', {})

        # Construct sets of all of the resources created and requested,
        # so that we can check for unused resources and default args
        all_resources_created = set()
        all_resources_requested = set()
        all_defaults_used = set()

        def validate_dag(dag, execution_context):
            nodes = Workflow.get_all_nodes(dag)

            dag_resources = {
                resource.name: resource for resource in dag['resources']
            }

            Workflow.ensure_no_duplicate_names(
                dag['name'],
                [node.name for node in nodes] + list(dag_resources),
                list(execution_context.resources))

            for resource in dag_resources.values():
                (create_properties, destroy_properties) = resource.resolve_properties(
                    execution_context=execution_context,
                    default_task_args=default_task_args,
                    base_operator_loader=plugins.manager.operators,
                    preprocessor_loader=plugins.manager.property_preprocessors,
                )
                all_defaults_used.update(set(create_properties.sources.default_task_args))
                all_defaults_used.update(set(destroy_properties.sources.default_task_args))

            all_resources_created.update(set(dag_resources))

            all_resources_requested.update(set(
                resource for node in nodes
                for resource in node.requires_resources))

            resources_available = execution_context.resources.copy()
            resources_available.update(dag_resources)

            missing_resources = {
                name: missing for (name, missing) in [
                    (
                        node.name,
                        frozenset(node.requires_resources) -
                        frozenset(resources_available)
                    ) for node in nodes]
                if missing
            }

            if missing_resources:
                raise ValueError(
                    'Error in dag {}: Operators require resources '
                    'outside their local contexts: {}'.format(
                        dag['name'],
                        missing_resources))

            for node in nodes:
                properties = node.resolve_properties(
                    execution_context=execution_context._replace(resources=resources_available),
                    default_task_args=default_task_args,
                    base_operator_loader=plugins.manager.operators,
                    preprocessor_loader=plugins.manager.property_preprocessors,
                )

                all_defaults_used.update(set(properties.sources.default_task_args))

            all_referrers = dag['sub_dags'] + dag['generators']
            for referrer in all_referrers:
                subdag = secondary_lookup[referrer.target]
                subdag_resources_available = {
                    name: resource for (name, resource) in six.iteritems(resources_available)
                    if name in frozenset(referrer.requires_resources)
                }
                subdag_ctx = ExecutionContext(
                    referrer=referrer,
                    resources=subdag_resources_available)
                validate_dag(subdag, subdag_ctx)

        validate_dag(spec.primary, ExecutionContext(referrer=None, resources={}))

        unused_resources = all_resources_created - all_resources_requested

        if unused_resources:
            raise ValueError('Unused resources `{}`'.format(
                '`, `'.join(unused_resources)))

        unused_defaults = frozenset(default_task_args) - frozenset(all_defaults_used)

        if unused_defaults:
            logger.warning(
                'Unused default task args: `%s`',
                '`, `'.join(unused_defaults))

    @staticmethod
    def get_all_nodes(dag):
        return \
            dag.get('operators', []) + \
            dag.get('before', []) + \
            dag.get('after', []) + \
            dag.get('sub_dags', []) + \
            dag.get('generators', [])

    @staticmethod
    def ensure_no_duplicate_names(dag_name, node_names, other_names_in_context=None):
        all_names = (other_names_in_context or []) + node_names
        dupes = [item for (item, count) in Counter(
            all_names).items() if count > 1]
        if dupes:
            message = None
            if other_names_in_context:
                message = 'Duplicated names found in DAG {} with execution contex `{}`: {}'.format(
                    dag_name, other_names_in_context, dupes)
            else:
                message = 'Duplicated names found in DAG {}: {}'.format(
                    dag_name, dupes)

            raise ValueError(message)

    @staticmethod
    def parse_dag(dag, schema_cls):
        """ Parse the DAG using the specified schema class.  Raise an exception
            if any errors are detected.
        """
        loaded = schema_cls().load(dag)
        if loaded.errors:
            dag_description = 'primary' if issubclass(
                schema_cls, PrimaryDagSchema) else 'sub'
            raise Exception('Found errors in {} dag: {}'.format(
                dag_description, loaded.errors))

        parsed = loaded.data

        registries = {
            'resources': plugins.manager.resources,
            'operators': plugins.manager.operators,
            'before': plugins.manager.operators,
            'after': plugins.manager.operators,
            'sub_dags': plugins.manager.subdags,
            'generators': plugins.manager.generators,
        }

        plugin_config = parsed.get('plugin_config')

        for (name, registry) in six.iteritems(registries):
            items = parsed.get(name, [])

            if name == 'before':
                items += plugins.manager.insert_before(plugin_config)
            elif name == 'operators':
                items += plugins.manager.insert_operators(plugin_config)
            elif name == 'after':
                items += plugins.manager.insert_after(plugin_config)

            parsed[name] = list(map(registry, items))

        imports = parsed.get('imports', {})
        imports.update(plugins.manager.insert_imports(plugin_config))
        if imports:
            parsed['imports'] = imports

        default_task_args = parsed.get('default_task_args', {})
        default_task_args.update(
            plugins.manager.insert_default_task_args(plugin_config))
        if default_task_args:
            parsed['default_task_args'] = default_task_args

        dag_args = parsed.get('dag_args', {})
        dag_args.update(plugins.manager.insert_dag_args(plugin_config))
        if dag_args:
            parsed['dag_args'] = dag_args

        return parsed

    def build_dag(
            self,
            builder_cls,
            sub_dag_builder=None,
            generator_builder=None):
        """ Build a DAG using the provided builder class
            :param builder_cls: a DagBuilderBase class
            :type builder_cls: class of boundary_layer.builders.base.DagBuilderBase
        """
        if not issubclass(builder_cls, DagBuilderBase):
            raise Exception(
                'Invalid builder_cls argument: {} (not a subclass of '
                'DagBuilderBase)!'.format(builder_cls))

        builder = builder_cls(
            dag=self.specs.parsed.primary,
            graph=self.specs.graphs.primary,
            reference_path=[self.specs.parsed.primary['name']],
            metadata=WorkflowMetadata(
                filename=self.filename, yaml_config=self.yaml_config),
            specs=self.specs,
            sub_dag_builder=sub_dag_builder,
            generator_builder=generator_builder)

        return builder.build()

    def prune(self, prune_nodes=None, only_nodes=None):
        """ Prunes the current workflow by removing nodes and/or keeping all but
            a prescribed set of nodes.

            :param prune_nodes: list of nodes to remove
            :type prune_nodes: list<string>
            :param only_nodes: list of nodes to keep
            :type only_nodes: list<string>
            :returns: a new Workflow object reflecting the specified pruning operations
            :rtype: Workflow
        """
        if not prune_nodes and not only_nodes:
            return self

        if prune_nodes and only_nodes:
            raise Exception(
                'Please specify only one of prune_nodes and only_nodes. '
                'Found prune_nodes == {}, only_nodes == {}'.format(
                    prune_nodes,
                    only_nodes))

        partitioned = self._partitioned_node_paths(prune_nodes or only_nodes)
        prune_paths = keep_paths = None

        if prune_nodes:
            (prune_paths, keep_paths) = partitioned
        else:
            (keep_paths, prune_paths) = partitioned
            (keep_paths, prune_paths) = self._unprune_referenced_sub_workflows(
                keep_paths,
                prune_paths)

        (pruned_primary, pruned_secondary) = self._prune_paths(
            prune_paths,
            keep_paths,
            allow_augmented_keep_nodes=bool(only_nodes))

        final_primary = self._prune_unused_resources(pruned_primary)
        final_secondary = [
            self._prune_unused_resources(dag) for dag in pruned_secondary]

        return Workflow(
            primary=final_primary,
            secondary=final_secondary,
            filename=self.filename,
            yaml_config=None)

    def _unprune_referenced_sub_workflows(self, keep_paths, prune_paths):
        """ This method is only called when --only-nodes was specified.  It
            enables a user to specify a sub-workflow referencing node as one of
            the --only-nodes arguments.  If such a node is referenced, then the
            entire sub-workflow that it targets will be added to the keep_paths
            list, and furthermore, any referring nodes nested in that sub-workflow
            will also be added to the keep_paths recursively.

            :param keep_paths: the keep paths computed after partitioning the workflow
            :type keep_paths: list<list<(string, string)>>
            :param prune_paths: the prune paths computed after partitioning the workflow
            :type prune_paths: list<list<(string, string)>>

            :returns: an updated pair of (keep_paths, prune_paths)
            :rtype: (list<list<(string, string)>>, list<list<(string, string)>>)
        """

        keep_nodes = frozenset([path[-1] for path in keep_paths])

        shift_path_indexes = frozenset(
            idx for (idx, path) in enumerate(prune_paths)
            if any(node in keep_nodes for node in path))

        if not shift_path_indexes:
            return (keep_paths, prune_paths)

        for idx in shift_path_indexes:
            node = prune_paths[idx][-1]
            logger.info(
                "Keeping node %s.%s because it is downstream of an --only-nodes argument",
                node[0],
                node[1])

        return self._unprune_referenced_sub_workflows(
            keep_paths + [prune_paths[i] for i in shift_path_indexes],
            [path for (i, path) in enumerate(prune_paths) if i not in shift_path_indexes])

    def _prune_paths(self, prune_nodes, keep_nodes, allow_augmented_keep_nodes):
        """ Actually do the pruning.  There are a few steps here.

            First, we must figure out whether there are any nodes in the keep
            pile that require nodes from the prune pile, because those nodes
            are used in paths to the keep nodes (but only if
            allow_augmented_keep_nodes is True)

            Second, we figure out whether there are any nodes in the keep pile
            that refer to sub-workflows that are entirely in the prune pile,
            so that these referring nodes should also be pruned.

            Third, we figure out which workflows, if any, will become inaccessible
            due to the removal of referring nodes, and we discard these.

            Fourth, we prune all the specified nodes out of the graph.

            Finally, we delete any sub-workflows that end up empty after pruning.

            :param prune_nodes: paths to all of the nodes we plan to prune
            :type prune_nodes: list<list<(string, string)>>
            :param keep_nodes: paths to all of the nodes we plan to keep
            :type keep_nodes: list<list<(string, string)>>
            :param allow_augmented_keep_nodes: Whether to augment the list of
                keep_nodes by adding in any nodes required for access to nodes
                in the keep_nodes list.  For example,
        """

        # Use a set to denote the nodes that we are ultimately going
        # to prune
        planned_prune_nodes = set(path[-1] for path in prune_nodes)
        planned_keep_nodes = set(path[-1] for path in keep_nodes)

        # First step, remove items from planned_prune_nodes if necessary
        if allow_augmented_keep_nodes:
            self._augment_keep_nodes_list(
                keep_nodes,
                planned_prune_nodes,
                planned_keep_nodes)

        # Second step, augment planned_prune_nodes to reflect any fully-deleted
        # sub-workflows
        self._augment_pruned_sub_workflow_referrers(
            planned_prune_nodes,
            planned_keep_nodes)

        # Third step, identify any sub-workflows that have become inaccessible
        # by removal of referring nodes
        inaccessible_workflows = self._find_inaccessible_workflows(
            planned_prune_nodes)

        # Fourth step, prune the graph
        logger.debug('going to prune away nodes: %s', planned_prune_nodes)

        keyed_graphs = self._build_keyed_graph_map()

        pruned_primary = self._prune_workflow(
            workflow=self.primary,
            is_primary=True,
            graph=keyed_graphs[None],
            prune_nodes=planned_prune_nodes)
        if self._workflow_is_empty(pruned_primary):
            raise Exception(
                'Pruning operation produced an empty primary workflow: {}'.format(pruned_primary))

        pruned_secondary = []
        for workflow in self.secondary:
            workflow_name = workflow['name']

            if workflow_name in inaccessible_workflows:
                logger.debug(
                    'Skipping inaccessible workflow %s', workflow_name)
                continue

            pruned_workflow = self._prune_workflow(
                workflow=workflow,
                is_primary=False,
                graph=keyed_graphs[workflow_name],
                prune_nodes=planned_prune_nodes)

            # Fifth step, discard empty workflows
            if self._workflow_is_empty(pruned_workflow):
                logger.info(
                    'Pruning operation produced an empty sub-workflow: %s',
                    pruned_workflow['name'])
            else:
                pruned_secondary.append(pruned_workflow)

        logger.debug('pruned primary workflow: %s', pruned_primary)
        logger.debug('pruned secondary workflows: %s', pruned_secondary)

        return (pruned_primary, pruned_secondary)

    @classmethod
    def _workflow_is_empty(cls, workflow):
        return not cls.get_all_nodes(workflow)

    @staticmethod
    def _augment_keep_nodes_list(keep_node_paths, planned_prune_nodes, planned_keep_nodes):
        """ Examines the paths to nodes in the keep_node_paths argument, making
            sure that elements of those paths will not be pruned out of the
            graph, making those nodes inaccessible.

            :param keep_node_paths: paths to the nodes we plan to keep
            :type keep_node_paths: list<list<(string, string)>>
            :param planned_prune_nodes: set of nodes currently planned for removal.
                The contents of this set will be modified by this method.
            :type planned_prune_nodes: set<(string, string)>
            :param planned_keep_nodes: set of nodes currently planned for inclusion
                in the pruned workflow.  The contents of this set will be modified
                by this method.
            :type planned_keep_nodes: set<(string, string)>
        """
        for path in keep_node_paths:
            for node in path:
                if node in planned_prune_nodes:
                    planned_prune_nodes.remove(node)
                    planned_keep_nodes.add(node)
                    logger.debug(
                        'Keeping node %s required for accessibility to node %s',
                        node,
                        path[-1])

        return

    def _augment_pruned_sub_workflow_referrers(
            self,
            planned_prune_nodes,
            planned_keep_nodes):
        """ Determine which, if any, workflows will be fully deleted by the
            prune operation, identify all nodes that refer to those workflows,
            and schedule them for deletion them as well.

            :param planned_prune_nodes: set of nodes currently planned for removal.
                The contents of this set will be modified by this method.
            :type planned_prune_nodes: set<(string, string)>
            :param planned_keep_nodes: set of nodes currently planned for inclusion
                in the pruned workflow.  The contents of this set will be modified
                by this method.
            :type planned_keep_nodes: set<(string, string)>
        """

        all_workflow_names = frozenset(dag['name'] for dag in self.secondary)
        deleted_workflows = all_workflow_names - \
            frozenset(node[0] for node in planned_keep_nodes)

        referrer_map = self._build_referrer_map()

        for wf in deleted_workflows:
            referrers = referrer_map[wf]
            logger.debug(
                'Pruning nodes %s that refer to fully-deleted workflow %s',
                referrers,
                wf)

            planned_prune_nodes |= set(referrers)
            planned_keep_nodes -= set(referrers)

        return

    def _find_inaccessible_workflows(self, prune_nodes):
        """ From the provided list of nodes to be pruned, determine which
            workflows will become inaccessible once the prune operation is
            complete.

            :param prune_nodes: set of nodes that will be removed
            :type prune_nodes: set<(string, string)>
            :returns: a list of names of workflows that will be inaccessible
            :rtype: frozenset<string>
        """

        referrer_map = self._build_referrer_map()

        removed_referring_nodes = frozenset(
            node for referrers in referrer_map.values()
            for node in referrers
            if node in prune_nodes)

        return frozenset(
            workflow for (workflow, referrers) in six.iteritems(referrer_map)
            if all(referrer in removed_referring_nodes for referrer in referrers))

    def _prune_workflow(self, workflow, is_primary, graph, prune_nodes):
        """ Produce the pruned workflow.  Works by pruning the provided graph
            using the provided prune_nodes argument, and then reconstructing
            the workflow to include only the nodes remaining after pruning,
            with their upstream dependencies set properly to those computed
            during pruning.

            :param workflow: the workflow to prune
            :type workflow: dict
            :param is_primary: whether or not this workflow is the primary
                workflow
            :type is_primary: boolean
            :param graph: the OperatorGraph associated with this workflow
            :type graph: OperatorGraph
            :param prune_nodes: the set of all nodes being pruned from all
                workflows, specified as (workflow_name, node_name) tuples
            :type prune_nodes: set<(string, string)>

            :returns: the pruned workflow
            :rtype: dict
        """
        prune_node_names = [
            node_name
            for (prune_workflow_name, node_name) in prune_nodes
            if (is_primary and prune_workflow_name is None) or
            (workflow['name'] == prune_workflow_name)
        ]

        logger.debug(
            'Graph for workflow %s (%s) before pruning: %s',
            workflow['name'],
            'primary' if is_primary else 'secondary',
            graph.graph.nodes())

        _GraphUtil.prune_nodes(
            graph=graph.graph,
            nodes=[graph.nodes[node_name] for node_name in prune_node_names])

        logger.debug(
            'Graph for workflow %s (%s) after pruning: %s',
            workflow['name'],
            'primary' if is_primary else 'secondary',
            graph.graph.nodes())

        return self._strip_workflow_nodes(workflow, graph.graph)

    def _strip_workflow_nodes(self, workflow, graph):
        """ Compute a new workflow from the provided one, including only those
            nodes present in the provided graph, and with upstream dependencies
            set according to those in the graph.

            :param workflow: the workflow
            :type workflow: dict
            :param graph: the graph
            :type graph: nx.DiGraph
            :returns: a new workflow
            :rtype: dict
        """
        original_workflow_node_names = frozenset(
            wf['name'] for wf in self.get_all_nodes(workflow))
        keyed_nodes = {node.name: node for node in graph.nodes()}

        def strip_section_operators(operators):
            result = []
            for operator in operators:
                if operator['name'] not in keyed_nodes:
                    continue

                dependencies = [
                    node.name
                    for node in graph.predecessors(keyed_nodes[operator['name']])
                    if node.name in original_workflow_node_names
                ]

                new_operator = operator.copy()
                new_operator['upstream_dependencies'] = dependencies
                if not dependencies:
                    new_operator.pop('upstream_dependencies')

                # Remove any downstream dependencies that may have been specified
                # in the original graph, because we will use upstream dependencies
                # (arbitarily) as the mechanism for specifying all dependencies
                if 'downstream_dependencies' in new_operator:
                    new_operator.pop('downstream_dependencies')

                result.append(new_operator)

            return result

        new_workflow = workflow.copy()

        for section_name in ['before', 'after', 'operators', 'generators', 'sub_dags']:
            if section_name not in workflow:
                continue

            new_section = strip_section_operators(workflow[section_name])
            if new_section:
                new_workflow[section_name] = new_section
                logger.debug('New workflow section %s: %s',
                             section_name, new_section)
            else:
                new_workflow.pop(section_name)
                logger.debug('Removing workflow section %s', section_name)

        return new_workflow

    def _build_keyed_workflow_map(self):
        result = {
            None: self.primary,
        }

        result.update({
            dag['name']: dag for dag in self.secondary
        })

        return result

    def _build_keyed_graph_map(self):
        result = {
            None: self.specs.graphs.primary,
        }

        result.update({
            graph.dag['name']: graph for graph in self.specs.graphs.secondary
        })

        return result

    def _build_referrer_map(self):
        """ Build a map from all subdags to the parent dags that refer to them

            :returns: a map from subdag target -> referring dags paired with
                node names in those dags
            :rtype: dict<string, list<(string, string)>>
        """
        result = {}

        dag_list = self._build_keyed_workflow_map().items()

        for (referrer, dag) in dag_list:
            for sub_dag in dag.get('sub_dags', []):
                result.setdefault(sub_dag['target'], [])
                result[sub_dag['target']].append((referrer, sub_dag['name']))

            for generator in dag.get('generators', []):
                result.setdefault(generator['target'], [])
                result[generator['target']].append(
                    (referrer, generator['name']))

        return result

    def _partitioned_node_paths(self, node_refs):
        """ Build a list of paths to all node references in the node_refs
            argument, by resolving all <parent>.<target> references to the
            actual parent and target DAGs

            :param node_refs: list of nodes to prune.  Format is either <node_name>
                (for nodes in the primary dag) or <dag_name>.<node_name> (for nodes
                in one of the subdags or generator dags)
            :type node_refs: list<string>
            :returns: a tuple of (paths to nodes referenced, paths to nodes not referenced)
            :rtype: tuple<list<list<(string, string)>>, list<list<(string, string)>>>
        """
        workflows = self._build_keyed_workflow_map()
        node_paths = self._all_node_paths()

        nodes = set()
        for node_ref in node_refs:
            path = node_ref.split('.')
            if len(path) > 2:
                raise Exception(
                    'Invalid node specification for nodes: {} (expected '
                    'a node spec of the form <workflow_name>.<node_name>, '
                    'or <node_name> for nodes in the primary workflow.'.format(
                        node_ref))

            workflow_name = None if len(path) == 1 else path[0]
            node_name = path[0 if len(path) == 1 else 1]

            node = (workflow_name, node_name)
            nodes.add(node)

            if workflow_name not in workflows:
                raise InvalidWorkflowReference(
                    'Workflow {} not found in the dag!'.format(workflow_name))

            if node not in node_paths:
                raise InvalidNodeReference(
                    'Node {} not found in workflow {}!'.format(
                        node_name,
                        workflow_name))

        referenced_paths = list(map(node_paths.__getitem__, nodes))
        complement_paths = [path for (node, path) in six.iteritems(
            node_paths) if node not in nodes]

        return (referenced_paths, complement_paths)

    def _all_node_paths(self):
        """ Build a mapping of the paths to all nodes in the graph, starting from
            the primary DAG.

            :returns: stuff
            :rtype: dict<string, string>
        """
        workflows = self._build_keyed_workflow_map()
        referrers = self._build_referrer_map()

        paths = {}

        for (workflow_name, workflow) in six.iteritems(workflows):
            for node in self.get_all_nodes(workflow):
                paths[(workflow_name, node['name'])] = \
                    self._get_path_to_node(
                        workflow_name, node['name'], referrers)

        return paths

    def _get_path_to_node(self, workflow, node, referrers):
        """ Find a path from the primary workflow to the specified workflow/node,
            where each path element is a tuple of (workflow name, referring node name)

            :param workflow: the name of the workflow
            :type workflow: string
            :param node: the name of the node
            :type node: string
            :param referrers: Referrer map, as generated by the method _build_referrer_map()
            :type referrers: dict<string, list<(string, string)>>
            :returns: a list of (workflow name, referring node name) pairs
            :rtype: list<(string, string)>
        """
        paths = []

        def _backtrack(_workflow, _node, path):
            if not _workflow or _workflow == self.primary['name']:
                paths.append([(None, _node)] + list(reversed(path)))
                return

            for (parent_workflow, parent_node) in referrers[_workflow]:
                if parent_workflow == self.primary['name']:
                    # To avoid double-counting, we only take paths that
                    # encode the primary workflow as None, rather than by
                    # its name
                    continue

                _backtrack(parent_workflow, parent_node,
                           path + [(_workflow, _node)])

        _backtrack(workflow, node, [])

        if len(paths) > 1:
            raise Exception(
                'Multiple paths found to node {}: cannot prune'.format(
                    ('{}.{}'.format(workflow, node)) if workflow else node))

        return paths[0]

    @staticmethod
    def _prune_unused_resources(dag):
        resources_defined = dag.get('resources')
        if not resources_defined:
            return dag

        resources_required = frozenset(
            resource
            for operator_section in ['before', 'operators', 'after', 'subdags', 'generators']
            for operator in dag.get(operator_section, [])
            for resource in operator.get('requires_resources', []))

        keep_resources = []

        for resource in resources_defined:
            if resource['name'] not in resources_required:
                logger.info(
                    'Discarding unused resource %s from dag %s',
                    resource['name'],
                    dag['name'])
                continue

            keep_resources.append(resource)

        result = dag.copy()
        result['resources'] = keep_resources
        return result
