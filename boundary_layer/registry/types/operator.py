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

import six
from boundary_layer.logger import logger
from boundary_layer.registry import ConfigFileRegistry, RegistryNode, NodeTypes
from boundary_layer.schemas.internal.operators import OperatorSpecSchema
from boundary_layer.schemas.dag import ImportSchema
from boundary_layer import validator, util
from boundary_layer.containers import PropertySources, ResolvedProperties
from boundary_layer.exceptions import MissingPreprocessorException


class OperatorNode(RegistryNode):
    type = NodeTypes.OPERATOR

    @property
    def operator_class(self):
        return self.config['operator_class']

    @property
    def operator_class_module(self):
        return self.config['operator_class_module']

    def __init__(self, config, item):
        super(OperatorNode, self).__init__(config=config, item=item)

        self._resolved_properties = None
        self._preprocessor_imports = None
        self._default_task_args = None

    def set_default_task_args(self, args):
        self._default_task_args = args or {}

    @property
    def resolved_properties(self):
        if not self._resolved_properties:
            raise Exception(
                'Cannot retrieve resolved properties for operator {}: '
                'resolve_properties() has not been called yet!'.format(
                    self))

        return self._resolved_properties

    @property
    def operator_args(self):
        """ Return a dictionary of arguments to apply to the operator constructor.
            This method accounts for the existence of default_task_args, by
            removing any operator args that will be set from the default task args.
            This way, any costly constructions in the default task args are not
            re-computed for each operator that uses them.
        """
        resolved = self.resolved_properties

        result = resolved.values.copy()

        for property_name in resolved.sources.default_task_args:
            if self._default_task_args[property_name] == resolved.values[property_name]:
                result.pop(property_name)

        return result

    def imports(self):
        if not self._resolved_properties:
            raise Exception(
                'Cannot retrieve imports for operator {}: '
                'resolve_properties() has not been called yet!'.format(
                    self))

        loaded = ImportSchema().load(self.config.get('imports', {}))

        assert not loaded.errors, \
            ('Internal error: processing `imports` config {} for '
             'operator `{}`').format(
                 self.config.get('imports', {}),
                 self.name)

        result = loaded.data

        if self.operator_class:
            result.setdefault('objects', [])
            result['objects'].append({
                'module': self.operator_class_module,
                'objects': [self.operator_class],
            })

        for item in self._preprocessor_imports.values():
            if 'objects' in item:
                result.setdefault('objects', [])
                result['objects'] += item['objects']

            if 'modules' in item:
                result.setdefault('modules', [])
                result['modules'] += item['modules']

        return result

    def resolve_properties(
            self,
            execution_context,
            default_task_args=None,
            base_operator_loader=None,
            preprocessor_loader=None):
        """ Get the properties / arguments for the operator, and split them
            according to their source.  Specifically, properties are provided
            to the operator by either the DAG config file, the resources
            available in the operator's context, any task defaults specified
            in the primary DAG, and the schema defaults, in that order of
            precedence.

            Once the properties are all resolved, this method then validates
            all of the resolved arguments against the task's schema.

            :param execution_context: the context in which this node is executed,
                specifically containing the available resources and the node
                that referred to this node, if any
            :type execution_context: boundary_layer.containers.ExecutionContext
            :param default_task_args: the default task args defined in the
                DAG
            :type default_task_args: dict
            :param base_operator_loader: A method that retrieves typed operators,
                equivalent to a Registry.get method
            :type base_operator_loader: callable
            :param preprocessor_loader: A method that retrieves typed preprocessors,
                equivalent to a Registry.get method
            :type preprocessor_loader: callable

            :returns: a mapping of property source to property key/value pairs
            :rtype: dict<dict<string, any>>
        """
        schema = self.get_schema(base_operator_loader)
        schema_properties = frozenset(schema.get('properties', {}).keys())

        self.set_default_task_args(default_task_args)

        (sources, property_values) = self._get_property_sources_and_values(
            schema_properties,
            execution_context)

        validated = validator.validate_and_fill_defaults(
            item=property_values,
            schema=schema)

        for key in validated:
            if key not in property_values:
                continue

            sources.schema.add(key)

        logger.debug('%s: validated partitioned properties: %s', self.name, sources)

        preprocessors = self._load_preprocessors(
            base_operator_loader,
            preprocessor_loader)

        self._preprocessor_imports = {
            pp_name: pp.imports()
            for (pp_name, pp) in six.iteritems(preprocessors)
        }

        preprocessed_values = self._apply_preprocessors(
            args=validated,
            preprocessors=preprocessors)

        if self._resolved_properties:
            if preprocessed_values != self._resolved_properties.values:
                raise Exception(
                    'resolve_properties() was already called for operator {}, '
                    'and different values were computed this time!  Found: {}, '
                    'expected: {}.  This was probably caused by repeated '
                    'references to a sub-dag or generator using different resource '
                    'contexts.  This is not presently supported!'.format(
                        self,
                        preprocessed_values,
                        self._resolved_properties.values))
            else:
                logger.warning(
                    'resolve_properties() was already called for operator %s, '
                    'but no differences in the computed properties were found.',
                    self)

        self._resolved_properties = ResolvedProperties(
            sources=sources,
            values=preprocessed_values)

        return self._resolved_properties

    def _get_property_sources_and_values(
            self,
            schema_properties,
            execution_context):
        """ For the provided set of properties, determine the values, and the
            sources of those values, for the current node.

            Value sources include:
                 - default task args defined in the DAG
                 - available resources
                 - properties defined in the DAG
                 - fixed values (set below)
                 - global defaults (set below)

            Note that this method does not validate these values against the
            schema, and therefore does not include any values that could be
            derived from the schema's default settings.

            :param schema_properties: the list of property names that are
                applicable to this node
            :type schema_properties: list<str>
            :param default_task_args: default arg values provided in the DAG
            :type default_task_args: dict
            :param execution_context: the execution context
            :type execution_context: boundary_layer.containers.ExecutionContext
            :returns: sources and values as a 2-tuple
            :rtype: (PropertySources, dict)
        """

        sources = PropertySources(
            dag=set(),
            default_task_args=set(),
            resources=set(),
            schema=set(),
            global_defaults=set(),
            fixed_args=set(),
            unknown_to_schema=set())

        property_values = {}

        resource_args = self._get_resource_args(execution_context)

        global_defaults = self._get_global_defaults(execution_context)

        fixed_args = self._get_fixed_args()

        # make sure that the user has not tried to specify values for any
        # fixed args; this prevents the user from trying to attach operators
        # to a different DAG, for instance (which does not make sense because
        # there is only one DAG)
        invalid_properties = [
            property_name for property_name in fixed_args
            if property_name in self.properties]

        if invalid_properties:
            raise Exception(
                'Illegal properties `{}` provided for operator `{}`: these '
                'properties are assigned fixed values by boundary-layer that '
                'cannot be overridden'.format(
                    '` ,`'.join(invalid_properties),
                    self))

        for property_name in schema_properties:
            if property_name in fixed_args:
                # Check fixed args first, because we do not allow these to be
                # set by the user
                value = fixed_args[property_name]
                logger.debug(
                    '%s: Inserting value `%s` for argument `%s` from fixed_args',
                    self.name, value, property_name)

                property_values[property_name] = value
                sources.fixed_args.add(property_name)
                continue

            if property_name in self.properties:
                logger.debug(
                    '%s: Property `%s` found in user-props', self.name, property_name)

                property_values[property_name] = self.properties[property_name]
                sources.dag.add(property_name)
                continue

            resource_hits = resource_args.get(property_name, {})

            if len(resource_hits) > 1:
                raise ValueError('Error in operator {}: Multiple available resources '
                                 'provide the argument {}: {}. Please specify a value or limit '
                                 'limit the resource scope'.format(
                                     self.name,
                                     property_name,
                                     resource_hits))

            if len(resource_hits) == 1:
                (resource_name, value) = resource_hits.popitem()

                logger.debug('%s: Inserting value `%s` for argument `%s` from resource `%s`',
                             self.name, value, property_name, resource_name)

                property_values[property_name] = value
                sources.resources.add(property_name)
                continue

            if property_name in self._default_task_args:
                value = self._default_task_args[property_name]
                logger.debug('%s: Inserting value `%s` for argument `%s` from default_task_args',
                             self.name, value, property_name)

                property_values[property_name] = value
                sources.default_task_args.add(property_name)
                continue

            if property_name in global_defaults:
                value = global_defaults[property_name]
                logger.debug('%s: Inserting value `%s` for argument `%s` from global defaults',
                             self.name, value, property_name)
                property_values[property_name] = value
                sources.global_defaults.add(property_name)
                continue

            logger.debug(
                '%s: No resources or defaults available for property `%s`',
                self.name,
                property_name)

        unknown_to_schema = [
            property_name for property_name in self.properties
            if property_name not in schema_properties]

        for property_name in unknown_to_schema:
            value = self.properties[property_name]
            logger.debug(
                '%s: Inserting value `%s` for user-property `%s` which is not '
                'part of the schema for this operator',
                self.name,
                value,
                property_name)
            property_values[property_name] = value
            sources.unknown_to_schema.add(property_name)

        return (sources, property_values)

    def _apply_preprocessors(self, args, preprocessors):
        """ Apply any necessary preprocessing to the alread-validated args.
            This must be the last step in case any preprocessors are defined on
            fields that are inserted by the schema defaults.
        """
        result = args.copy()

        for (property_name, preprocessor) in six.iteritems(preprocessors):
            if property_name not in args:
                continue

            processed_value = preprocessor.process_arg(
                args[property_name],
                node=self,
                raw_args=args)
            logger.debug(
                'Property `%s` raw value: `%s`, processed value: `%s`',
                property_name,
                args[property_name],
                processed_value)

            result[property_name] = processed_value

        return result

    def _get_resource_args(self, execution_context):
        resources_available = self._get_resources_available(execution_context)
        result = {}

        for (resource_name, resource) in six.iteritems(resources_available):
            for (property_name, value) in six.iteritems(resource.get_provided_args()):
                result.setdefault(property_name, {})
                result[property_name][resource_name] = value

        return result

    def _get_resources_available(self, execution_context):
        keys_available = self.requires_resources & frozenset(execution_context.resources)
        return {key: execution_context.resources[key] for key in keys_available}

    def _load_preprocessors(self, base_loader, preprocessor_loader):
        def aggregator(previous_result, node):
            return previous_result + node.config.get('property_preprocessors', [])
        preprocessor_configs = self._aggregate_over_hierarchy(
            base_loader=base_loader,
            initial_value=self.config.get('property_preprocessors', []),
            aggregator=aggregator)

        if not preprocessor_configs:
            return {}

        if not preprocessor_loader:
            raise MissingPreprocessorException(
                'Node {} of type {} requires preprocessors {}, but no '
                'preprocessor_loader is available!'.format(
                    self,
                    self.type,
                    [config['type'] for config in preprocessor_configs]))

        result = {}
        for preprocessor_conf in preprocessor_configs:
            preprocessor = preprocessor_loader(preprocessor_conf)

            for property_name in preprocessor_conf['apply_to_properties']:
                result[property_name] = preprocessor
        return result

    def _get_fixed_args(self):
        return {
            'dag': '<<dag>>',
        }

    def _get_global_defaults(self, execution_context):
        return {
            'task_id': self._build_task_id(execution_context),
        }

    def _build_task_id(self, execution_context):
        base_name = util.sanitize_operator_name(self.name)
        if not execution_context.referrer or execution_context.referrer.type != NodeTypes.GENERATOR:
            return base_name

        suffix_mode = execution_context.referrer.item.get('auto_task_id_mode')
        if not suffix_mode or suffix_mode == 'item_name':
            return base_name + '-<<item_name>>'
        elif suffix_mode == 'index':
            return base_name + '-<<str(index)>>'

        raise Exception(
            'Unknown suffix_mode `{}` for generator `{}` found while processing '
            'node `{}`'.format(
                suffix_mode,
                execution_context.referrer.name,
                self.name))


class OperatorRegistry(ConfigFileRegistry):
    node_cls = OperatorNode
    spec_schema_cls = OperatorSpecSchema
