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
import xmltodict
import networkx as nx
import six

from boundary_layer.graph import _GraphUtil
from boundary_layer.logger import logger
from boundary_layer import exceptions, VERSION_STRING, plugins
from boundary_layer.oozier.jsp_macros import JspMacroTranslator
from boundary_layer.oozier.schema import OozieWorkflowSchema
from boundary_layer.oozier.actions import OozieSubWorkflowBuilderBase
from boundary_layer.workflow import Workflow


class OozieWorkflowParser(object):
    def __init__(
            self,
            file_fetcher,
            prune_forks=False,
            prune_joins=False,
            production=False,
            debug=False,
    ):

        self.file_fetcher = file_fetcher
        self.prune_forks = prune_forks
        self.prune_joins = prune_joins
        self.production = production
        self.debug = debug

    def load_workflow(self, primary_workflow_name, args):
        oozie_config = plugins.manager.get_oozie_config(args)

        cluster_config = oozie_config.cluster_config()

        (primary_dag, sub_dags) = self._parse_and_build_dags(
            primary_workflow_name,
            cluster_config,
            oozie_config)

        if self._requires_cluster_resource(primary_dag, sub_dags, cluster_config):
            primary_dag = self._insert_cluster_resource(
                primary_dag, cluster_config)

        primary_dag['compatibility_version'] = VERSION_STRING

        if oozie_config.plugin_config():
            primary_dag['plugin_config'] = oozie_config.plugin_config()

        if oozie_config.default_task_args():
            primary_dag['default_task_args'] = oozie_config.default_task_args()

        if oozie_config.dag_args():
            primary_dag['dag_args'] = oozie_config.dag_args()

        if oozie_config.dag_imports():
            primary_dag['imports'] = oozie_config.dag_imports()

        primary_dag['name'] = args.dag_name or primary_dag['name']

        # Build the workflow
        wf = Workflow(
            primary=primary_dag,
            secondary=list(sub_dags.values()),
            filename='oozie:' + primary_workflow_name,
        )

        # Prune away any unwanted nodes
        if args.prune_nodes or args.only_nodes:
            wf = wf.prune(
                prune_nodes=args.prune_nodes,
                only_nodes=args.only_nodes)

        # Now add in any additional operators that we require.  We have to do
        # this down here in order to avoid having these nodes pruned out above.
        if oozie_config.upstream_operators():
            primary_copy = wf.primary.copy()
            primary_copy['before'] = oozie_config.upstream_operators()
            wf = Workflow(
                primary=primary_copy,
                secondary=wf.secondary,
                filename='oozie:' + primary_workflow_name)

        return wf

    @staticmethod
    def partition_actions(data):
        operators = [ac for ac in data['action'] if not isinstance(ac, OozieSubWorkflowBuilderBase)]
        sub_workflows = [ac for ac in data['action'] if isinstance(ac, OozieSubWorkflowBuilderBase)]

        logger.debug('Partitioned actions for workflow %s.  Operators are %s, sub_workflows are %s',
                     data['name'],
                     [operator.name for operator in operators],
                     [sub_workflow.name for sub_workflow in sub_workflows])

        assert len(operators + sub_workflows) == len(data['action']), \
            'Some actions were not partitioned!'

        return {
            'operators': operators,
            'sub_workflows': sub_workflows,
        }

    def _parse_workflow(self, filename, cluster_config, oozie_config):
        parsed = xmltodict.parse(
            self.file_fetcher.fetch_file_content(filename))

        loaded = OozieWorkflowSchema(context={
            'cluster_config': cluster_config,
            'oozie_plugin': oozie_config,
            'macro_translator': JspMacroTranslator(oozie_config.jsp_macros()),
            'production': self.production,
        }).load(parsed)

        if loaded.errors:
            raise Exception('Errors parsing file {}: {}'.format(
                filename,
                loaded.errors))

        data_copy = loaded.data.copy()
        data_copy.update(self.partition_actions(loaded.data))

        return data_copy

    @staticmethod
    def _sub_workflow_target_name(sub_workflow_action):
        return '/'.join(sub_workflow_action['app_path'].split('/')[3:])

    def _parse_all(
            self,
            primary_workflow_path_name,
            cluster_config,
            oozie_config):

        def _build_workflow_path(name):
            return os.path.join(name, 'workflow.xml')

        path = _build_workflow_path(primary_workflow_path_name)
        logger.debug('parsing primary workflow from path %s', path)
        primary = self._parse_workflow(path, cluster_config, oozie_config)

        sub_workflows = {}
        parsed_sub_workflow_targets = set()
        q = [wf for wf in primary['sub_workflows']]

        while q:
            item = q.pop()
            swf_path_name = self._sub_workflow_target_name(item.get_action())
            if swf_path_name in parsed_sub_workflow_targets:
                continue

            parsed_sub_workflow_targets.add(swf_path_name)

            swf_path = _build_workflow_path(swf_path_name)

            logger.debug('parsing sub workflow from path %s', swf_path)
            swf = self._parse_workflow(
                swf_path, cluster_config, oozie_config)

            sub_workflows[swf['name']] = swf
            item.set_target_name(swf['name'])

            q += [wf for wf in swf['sub_workflows']]

        return (primary, sub_workflows)

    def _flow_control_nodes(self, wf):
        result = {
            'start': ([wf['start']['to']], wf['start']),
            wf['end'].get('name', 'end'): ([], wf['end'])
        }

        if wf.get('kill'):
            result[wf['kill'].get('name', 'kill')] = ([], wf['kill'])

        for decision in wf['decision']:
            if not self.debug:
                raise Exception('decision node found: `{}` not supported right '
                                'now!  Set --debug to build without raising this error, but '
                                'only at your own risk!'.format(decision['name']))

            logger.warning(
                'decision node found: `%s`.  Including all downstream '
                'branches because --debug was specified', decision['name'])

            cases = [case['to'] for case in decision['switch']['case']]
            default = decision['switch']['default']['to']
            result[decision['name']] = (cases + [default], decision)

        return result

    @staticmethod
    def _fork_nodes(wf):
        return {
            fork['name']: ([path['start'] for path in fork['path']], fork)
            for fork in wf['fork']
        }

    @staticmethod
    def _join_nodes(wf):
        return {join['name']: ([join['to']], join) for join in wf['join']}

    @staticmethod
    def _operator_nodes(wf):
        return {op.name: ([op.ok_to], op) for op in wf['operators']}

    @staticmethod
    def _sub_workflow_nodes(wf):
        return {swf.name: ([swf.ok_to], swf) for swf in wf['sub_workflows']}

    def _all_nodes(self, wf):
        result = {}
        result.update(self._flow_control_nodes(wf))
        result.update(self._fork_nodes(wf))
        result.update(self._join_nodes(wf))
        result.update(self._operator_nodes(wf))
        result.update(self._sub_workflow_nodes(wf))

        return result

    @staticmethod
    def _build_graph(graph_name, nodes):
        graph = nx.DiGraph()

        logger.debug('Creating graph %s from nodes %s',
                     graph_name, nodes.keys())

        graph.add_nodes_from(nodes.keys())

        graph.add_edges_from(
            (upstream_name, downstream_name)
            for (upstream_name, (downstream_node_names, node)) in six.iteritems(nodes)
            for downstream_name in downstream_node_names)

        if not nx.algorithms.components.is_weakly_connected(graph):
            components = list(
                nx.algorithms.components.weakly_connected_components(graph))
            logger.warning(
                'Multiple connected components found for graph `%s`: %s',
                graph_name,
                components)

        if not nx.algorithms.dag.is_directed_acyclic_graph(graph):
            raise exceptions.CyclicWorkflowException(
                'Invalid graph `{}`: not a DAG!'.format(graph_name))

        logger.debug('Successfully created graph %s with nodes %s',
                     graph_name, graph.nodes)

        return graph

    def _prune_flow_control_nodes(self, wf, graph):
        prune_nodes = list(self._flow_control_nodes(wf).keys())

        if self.prune_forks:
            prune_nodes += list(self._fork_nodes(wf).keys())

        if self.prune_joins:
            prune_nodes += list(self._join_nodes(wf).keys())

        return _GraphUtil.prune_nodes(graph, nodes=prune_nodes)

    def _build_dag(self, wf):
        graph = self._build_graph(wf['name'], self._all_nodes(wf))

        self._prune_flow_control_nodes(wf, graph)

        operator_nodes = self._operator_nodes(wf)

        fork_join_nodes = self._fork_nodes(wf)
        fork_join_nodes.update(self._join_nodes(wf))

        sub_workflow_nodes = self._sub_workflow_nodes(wf)

        operators = []
        sub_workflows = []

        # Build in dependency-resolved order, for easier viewing by humans
        for node_name in nx.algorithms.dag.topological_sort(graph):
            upstream = list(graph.predecessors(node_name))
            base_operator = {
                'upstream_dependencies': upstream} if upstream else {}

            node = fork_join_nodes.get(node_name)
            if node:
                base_operator.update(node[1]['operator'])
                base_operator['name'] = node[1]['name']  # must take the oozie action name
                operators.append(base_operator)
                continue

            node = operator_nodes.get(node_name)
            if node:
                base_operator.update(node[1].get_operator())
                operators.append(base_operator)
                continue

            swf = sub_workflow_nodes.get(node_name)
            if not swf:
                raise Exception('Unrecognized node name: `{}` (swf nodes are {})'.format(
                    node_name, list(sub_workflow_nodes.keys())))

            base_operator.update(swf[1].get_operator())

            sub_workflows.append(base_operator)

        result = {
            'name': wf['name'],
        }

        if operators:
            result['operators'] = operators

        if sub_workflows:
            result['sub_dags'] = sub_workflows

        return result

    def _parse_and_build_dags(
            self,
            primary_workflow_name,
            cluster_config,
            oozie_plugin):

        (primary, sub) = self._parse_all(
            primary_workflow_name,
            cluster_config,
            oozie_plugin)

        primary_dag = self._build_dag(primary)

        sub_dags = {}
        for (name, swf) in six.iteritems(sub):
            sub_dags[name] = self._build_dag(swf)

        return self._fixup_dags(primary_dag, sub_dags, cluster_config)

    def _fixup_dags(self, primary_dag, sub_dags, cluster_config):
        fixedup_primary_dag = self._fixup_subdags(
            primary_dag, sub_dags, cluster_config)

        # re-key the sub_dags by their actual name, rather than by their
        # path name
        fixedup_sub_dags = {
            dag['name']: dag for dag in
            [self._fixup_subdags(sub_dag, sub_dags, cluster_config)
             for sub_dag in sub_dags.values()]
        }

        return (fixedup_primary_dag, fixedup_sub_dags)

    def _fixup_subdags(self, dag, all_sub_dags, cluster_config):
        """
            Once we have loaded the primary dag and all of the sub-dags in
            isolation, we have to fill-in some values that can only be
            determined collectively.  For now, this includes only:
            1. We must make sure that any resources required by a subdag are
               passed to it from the parent dag (e.g. a dataproc cluster that
               is created by the primary but used to run jobs in the subdag)
        """
        subdags = dag.get('sub_dags')
        if not subdags:
            return dag

        copy = dag.copy()
        copy['sub_dags'] = [sd.copy() for sd in subdags]
        for sd in copy['sub_dags']:
            target_subdag = all_sub_dags.get(sd['target'])
            if self._requires_cluster_resource(target_subdag, all_sub_dags, cluster_config):
                sd['requires_resources'] = [cluster_config.managed_resource['name']]

        return copy

    def _requires_cluster_resource(self, dag, sub_dags, cluster_config):
        if not cluster_config.managed_resource:
            return False

        resource_name = cluster_config.managed_resource['name']
        return any(resource_name in op.get('requires_resources', [])
                   for op in dag.get('operators', [])) or \
            any(self._requires_cluster_resource(sub_dags[sd['target']], sub_dags, cluster_config)
                for sd in dag.get('sub_dags', []))

    @staticmethod
    def _insert_cluster_resource(wf, cluster_config):
        copy = wf.copy()
        copy['resources'] = [cluster_config.managed_resource]

        return copy
