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

import abc
from six.moves import filter
from jinja2 import Environment, PackageLoader
from boundary_layer.builders import util
from boundary_layer.logger import logger
from boundary_layer.registry import NodeTypes
from boundary_layer.util import sanitize_operator_name
from boundary_layer.containers import WorkflowMetadata


class DagBuilderBase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def indent_operators(self):
        pass

    @abc.abstractmethod
    def preamble(self):
        pass

    @abc.abstractmethod
    def epilogue(self):
        pass

    @staticmethod
    def _build_jinja_env():
        jenv = Environment(
            loader=PackageLoader('boundary_layer', 'builders/templates'),
            trim_blocks=True)

        jenv.filters['format_value'] = util.format_value
        jenv.filters['add_leading_spaces'] = util.add_leading_spaces
        jenv.filters['comment'] = util.comment
        jenv.filters['sanitize_operator_name'] = sanitize_operator_name
        jenv.filters['verbatim'] = util.verbatim

        return jenv

    def get_jinja_template(self, template_filename):
        return self._build_jinja_env().get_template(template_filename)

    def get_imports(self):
        all_nodes = self.specs.graphs.primary.ordered() + \
            [node for graph in self.specs.graphs.secondary
             for node in graph.ordered()]

        all_imports = [self.dag.get('imports', {})] + \
            [node.imports() for node in all_nodes]

        objects = {}
        modules = set()
        for node_imports in all_imports:
            modules |= set(node_imports.get('modules', []))

            for item in node_imports.get('objects', []):
                objects.setdefault(item['module'], set())
                objects[item['module']] |= set(item['objects'])

        return {
            'modules': modules,
            'objects': objects,
        }

    def __init__(
            self,
            dag,
            graph,
            reference_path,
            specs,
            metadata=None,
            referring_node=None,
            sub_dag_builder=None,
            generator_builder=None):
        self.dag = dag
        self.graph = graph
        self.reference_path = reference_path
        self.specs = specs
        self.metadata = metadata or WorkflowMetadata(None, None)
        self.referring_node = referring_node

        self.sub_dag_builder = sub_dag_builder
        self.generator_builder = generator_builder

    @property
    def default_task_args(self):
        return self.specs.parsed.primary.get('default_task_args', {})

    def build_dag_id(self):
        return util.construct_dag_name(self.reference_path)

    def render_operator(self, node):
        template_filename = None
        if node.type == NodeTypes.GENERATOR:
            template_filename = 'generator_operator.j2'
        elif node.type == NodeTypes.SUBDAG:
            template_filename = 'subdag_operator.j2'
        else:
            template_filename = 'operator.j2'

        template = self.get_jinja_template(template_filename)

        # Do not set upstream/downstream dependencies that involve generator nodes
        # at this stage; those are all set within the generator nodes, and if they are
        # set here, there will be python errors due to references to operators that
        # do not exist (generators do not correspond to operators)
        generator_nodes = frozenset(
            gen.name for gen in self.graph.graph.nodes
            if gen.type == NodeTypes.GENERATOR)

        upstream_deps = frozenset(
            dep.name for dep in self.graph.upstream_dependency_set(node))

        if generator_nodes & upstream_deps:
            logger.debug(
                'Not passing upstream generator dependencies `%s` to '
                'operator template for node `%s`',
                generator_nodes & upstream_deps,
                node.name)

        downstream_deps = frozenset(
            dep.name for dep in self.graph.downstream_dependency_set(node))

        if generator_nodes & downstream_deps:
            logger.debug(
                'Not passing downstream generator dependencies `%s` to '
                'operator template for node `%s`',
                generator_nodes & downstream_deps,
                node.name)

        return template.render(
            node=node,
            args=node.operator_args,
            upstream_dependencies=list(upstream_deps - generator_nodes),
            downstream_dependencies=list(downstream_deps - generator_nodes),
        )

    def get_secondary_dag(self, target):
        hits = [dag for dag in self.specs.parsed.secondary
                if dag['name'] == target]

        if not hits:
            raise ValueError('Secondary dag id {} not found'.format(target))

        if len(hits) > 1:
            raise ValueError(
                'Multiple hits for secondary dag id {}'.format(target))

        return hits[0]

    def get_secondary_graph(self, target):
        """ Get the graph corresponding to the target.  This is kind of ugly,
            a consequence of the way in which we currently store dags separately
            from graphs.  Ideally there would be only one of the two methods,
            get_secondary_(dag|graph).
        """
        self.get_secondary_dag(target)  # does the checking

        for (idx, dag) in enumerate(self.specs.parsed.secondary):
            if dag['name'] == target:
                return self.specs.graphs.secondary[idx]

        raise Exception("should not be possible")

    def get_target_builder_cls(self, node_type):
        if node_type == NodeTypes.GENERATOR:
            if not self.generator_builder:
                raise Exception('No generator builder is defined!')

            return self.generator_builder

        elif node_type == NodeTypes.SUBDAG:
            if not self.sub_dag_builder:
                raise Exception('No sub_dag builder is defined!')

            return self.sub_dag_builder

        raise Exception(
            'Node type `{}` has no known target builder'.format(
                node_type))

    def render_target(self, node):
        builder = self.get_target_builder_cls(node.type)(
            dag=self.get_secondary_dag(node.target),
            graph=self.get_secondary_graph(node.target),
            reference_path=self.reference_path + [node.name],
            specs=self.specs,
            referring_node=node,
            sub_dag_builder=self.sub_dag_builder,
            generator_builder=self.generator_builder,
            )

        return builder.build()

    def build(self):
        # Keep track of which subdag and generator targets have been rendered.
        # These targets can be reused by multiple referring nodes.
        rendered_targets = set()

        # We build the result by appending components to an array and then
        # joining together at the end
        components = [self.preamble()]

        # generators are rendered last, because they refer to both upstream and
        # downstream components when they express their dependencies
        generator_components = []

        for node in self.graph.ordered():
            operator = None
            if node.type in set([NodeTypes.GENERATOR, NodeTypes.SUBDAG]) \
                    and node.target not in rendered_targets:

                operator = '\n'.join([
                    self.render_target(node),
                    self.render_operator(node)])

                rendered_targets.add(node.target)

            elif node.type in NodeTypes:
                operator = self.render_operator(node)

            else:
                raise Exception(
                    'Unrecognized operator type: {}'.format(node.type))

            # add the rendered operator to the appropriate components list
            (components if node.type != NodeTypes.GENERATOR else generator_components).append(
                util.add_leading_spaces(
                    operator,
                    1 if self.indent_operators else 0))

        components += generator_components
        components.append(self.epilogue())

        return '\n'.join(filter(None, components))
