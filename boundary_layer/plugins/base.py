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
from boundary_layer.logger import logger
from boundary_layer.plugins.oozie_plugin import BaseOozieParserPlugin

from boundary_layer.registry.types import \
    OperatorRegistry, \
    SubdagRegistry, \
    GeneratorRegistry, \
    ResourceRegistry, \
    PropertyPreprocessorRegistry


class PluginPriority(Enum):
    FALLBACK = -1  # If all else fails...
    DEFAULT = 0  # The default plugin only, most likely
    SHARED = 1  # Can be overridden, but can override default
    FINAL = 2  # Cannot be overridden


class BasePlugin(object):
    __metaclass__ = abc.ABCMeta

    priority = PluginPriority.SHARED

    property_preprocessors = []

    def __init__(self):
        logger.debug('Loading plugin %s', self.name)

        if not isinstance(self.oozie_plugin_cls, type(None)) and not \
                issubclass(self.oozie_plugin_cls, BaseOozieParserPlugin):
            raise Exception('Invalid oozie plugin: {}'.format(
                self.oozie_plugin_cls))

        self.operator_registry = self.load_config_registry(
            'operators', OperatorRegistry)
        self.subdag_registry = self.load_config_registry('subdags', SubdagRegistry)
        self.generator_registry = self.load_config_registry(
            'generators', GeneratorRegistry)
        self.resource_registry = self.load_config_registry(
            'resources', ResourceRegistry)
        self.property_preprocessor_registry = \
            PropertyPreprocessorRegistry(self.property_preprocessors)

    @abc.abstractproperty
    def name(self):
        pass

    configs_path = None

    # Overrideable attribute for specifying an oozie parser plugin class
    # from the main plugin instance
    oozie_plugin_cls = None

    # Overrideable attribute for specifying a schema to use to parse this
    # plugin's entry in the plugin_config field of a DAG.  If set to None,
    # then an error will be raised if there are any entries in plugin_config
    # for this plugin.
    config_schema_cls = None

    def insert_imports(self, config):
        pass

    def insert_default_task_args(self, config):
        pass

    def insert_dag_args(self, config):
        pass

    def insert_before(self, config):
        pass

    def insert_operators(self, config):
        pass

    def insert_after(self, config):
        pass

    @classmethod
    def load_config_registry(cls, subpath, factory):
        logger.debug('Configs path: %s', cls.configs_path)
        if not cls.configs_path:
            return None

        registry_config_path = os.path.join(cls.configs_path, subpath)
        if not os.path.isdir(registry_config_path):
            return None

        return factory([registry_config_path])
