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
from boundary_layer.registry import Registry, RegistryNode, NodeTypes


class PropertyPreprocessor(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def process_arg(self, arg, node, raw_args):
        pass

    @abc.abstractproperty
    def type(self):
        pass

    def imports(self):
        return {}

    def __init__(self, properties):
        if not self.properties_schema_cls and properties:
            raise Exception(
                'Invalid properties `{}`: preprocessor `{}` does not '
                'take any properties!'.format(
                    properties,
                    self.type))

        if not self.properties_schema_cls:
            self.properties = None
            return

        loaded = self.properties_schema_cls().load(properties)
        if loaded.errors:
            raise Exception(
                'Error parsing properties for preprocessor `{}`: {}'.format(
                    self.type,
                    loaded.errors))

        self.properties = loaded.data

    @property
    def properties_schema_cls(self):
        """ Any preprocessor that requires a configuration must define a marshmallow
            schema for that configuration.  If no schema is provided, then
            we will reject any properties that are provided.
        """
        pass


class PropertyPreprocessorNode(RegistryNode):
    type = NodeTypes.PREPROCESSOR

    @property
    def configured_preprocessor(self):
        return self.config(self.item.get('properties', {}))

    def process_arg(self, arg, node, raw_args):
        return self.configured_preprocessor.process_arg(arg, node, raw_args)

    def imports(self):
        return self.configured_preprocessor.imports()


class PropertyPreprocessorRegistry(Registry):
    node_cls = PropertyPreprocessorNode

    @property
    def node_configs(self):
        return self._node_configs

    def __init__(self, preprocessor_classes):
        self._node_configs = {
            pp_cls.type: pp_cls for pp_cls in preprocessor_classes
        }
