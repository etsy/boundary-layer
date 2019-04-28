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
import os
from enum import Enum
import yaml

from boundary_layer.logger import logger
from boundary_layer import util
from boundary_layer.exceptions import DuplicateRegistryConfigName, InvalidConfig


class NodeTypes(Enum):
    OPERATOR = 'operator'
    RESOURCE = 'resource'
    GENERATOR = 'generator'
    SUBDAG = 'sub_dag'
    PREPROCESSOR = 'preprocessor'


class RegistryNode(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def type(self):
        pass

    @abc.abstractmethod
    def imports(self):
        pass

    def __init__(self, config, item):
        self.config = config
        self.item = item

        if self.type not in NodeTypes:
            raise ValueError('Invalid package type `{}`'.format(self.type))

        self._icebox = None

    @property
    def name(self):
        return self.item['name']

    @property
    def upstream_dependencies(self):
        return frozenset(self.item.get('upstream_dependencies', []))

    @property
    def downstream_dependencies(self):
        return frozenset(self.item.get('downstream_dependencies', []))

    @property
    def requires_resources(self):
        return frozenset(self.item.get('requires_resources', []))

    @property
    def properties(self):
        return self.item.get('properties', {})

    def _aggregate_over_hierarchy(self, base_loader, initial_value, aggregator):
        stack = [self]
        parent_set = set()
        result = initial_value
        while stack:
            node = stack.pop()
            schema_extends = node.config.get('schema_extends')
            if not schema_extends:
                continue

            if schema_extends in parent_set:
                raise Exception(
                    'Circular reference in schema_extends for node {}: '
                    'repeated parent {}'.format(self, schema_extends))

            parent_set.add(schema_extends)

            parent_node = base_loader({'type': schema_extends})
            stack.append(parent_node)

            result = aggregator(result, parent_node)

        return result

    def get_schema(self, base_loader):
        def aggregator(previous_result, node):
            schema = node.config.get('parameters_jsonschema', {})

            return util.merge_schemas(parent=schema, child=previous_result)

        return self._aggregate_over_hierarchy(
            base_loader=base_loader,
            initial_value=self.config.get('parameters_jsonschema', {}),
            aggregator=aggregator)

    def freeze(self):
        if self._icebox:
            return self._icebox

        self._icebox = (util.freeze(self.config), util.freeze(self.item))
        return self._icebox

    def __hash__(self):
        return hash(self.freeze())

    def __eq__(self, that):
        return isinstance(that, self.__class__) and \
            self.freeze() == that.freeze()

    def __ne__(self, that):
        return not self.__eq__(that)

    def __repr__(self):
        return '{self.__class__.__name__}({self.name})'.format(self=self)


class Registry(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def node_cls(self):
        pass

    @abc.abstractproperty
    def node_configs(self):
        pass

    def get(self, item):
        if 'type' not in item:
            raise Exception(
                'Invalid item: no `type` specified for `{}`'.format(item))

        if item['type'] not in self.node_configs:
            logger.debug('Unrecognized type `%s` for item %s',
                         item['type'], item)
            return None

        return self.node_cls(config=self.node_configs[item['type']], item=item)


class ConfigFileRegistry(Registry):
    @abc.abstractproperty
    def spec_schema_cls(self):
        pass

    def __init__(self, config_paths):
        self.config_paths = config_paths

        self._node_configs = self.load_configs(config_paths)

        if not issubclass(self.node_cls, RegistryNode):
            raise Exception('Invalid `node_cls` property {} (must be a '
                            'subclass of RegistryNode)'.format(
                                self.node_cls))

    @property
    def node_configs(self):
        return self._node_configs

    def load_from_file(self, filename):
        item = None
        with open(filename) as _in:
            item = yaml.safe_load(_in)

        logger.debug('validating item %s against schema %s',
                     item, self.spec_schema_cls.__name__)

        loaded = self.spec_schema_cls().load(item)
        if loaded.errors:
            raise InvalidConfig('Invalid config spec in file {}: {}'.format(
                filename, loaded.errors))

        return loaded.data

    def load_configs(self, config_paths):
        if not isinstance(config_paths, list):
            raise TypeError(
                'Invalid `config_paths` argument: expected a '
                'list<str>, got {}'.format(config_paths))

        registry = {}
        duplicates = {}

        for path in config_paths:
            logger.debug('Loading configs from path %s', path)
            for filename in os.listdir(path):
                full_path = os.path.abspath(os.path.join(path, filename))

                if os.path.splitext(filename)[1] not in ['.yaml', '.yml']:
                    raise Exception(
                        'Invalid file found: {} (must be a yaml config file)'.format(
                            full_path))
                config = self.load_from_file(full_path)

                name = config['name']

                if name in registry:
                    duplicates.setdefault(name, [registry[name]])
                    duplicates[name].append(config)
                    continue

                registry[name] = config

        if duplicates:
            raise DuplicateRegistryConfigName(
                'Duplicate names found while loading registry: `{}` '
                '(full configurations: {})'.format(
                    '`, `'.join(duplicates.keys()),
                    duplicates))

        return registry
