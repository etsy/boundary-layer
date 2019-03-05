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

import operator
import pkg_resources
import marshmallow as ma
from boundary_layer.plugins.oozie_plugin import OozieMetaPlugin
from boundary_layer.logger import logger


class PluginManager(object):
    ENTRY_POINT_GROUP = 'boundary_layer_plugins'

    def __init__(
            self,
            load_package_plugins=True,
            plugins=None):

        self._plugins = [p for p in (plugins or [])]

        if load_package_plugins:
            self._plugins += self._load_package_plugins()

        logger.info('Loaded plugins %s', ', '.join(
            plugin.name for plugin in self._plugins))

    def validate_config(self, plugin_config):
        plugin_names = frozenset(plugin.name for plugin in self._plugins)

        bad_plugin_names = [
            name for name in plugin_config if name not in plugin_names]
        if bad_plugin_names:
            raise Exception(
                'Unrecognized plugin name(s) `{}` in plugin config; perhaps '
                'these plugins are not installed?'.format(
                    '`, `'.join(bad_plugin_names)))

        for plugin in self._plugins:
            self.parse_plugin_config(plugin, plugin_config.get(plugin.name, {}))

    def parse_plugin_config(self, plugin, config):
        if not plugin.config_schema_cls and config:
            raise Exception(
                'Config provided for plugin {}, but this plugin does not have '
                'a config schema!'.format(plugin.name))

        if not plugin.config_schema_cls:
            return None

        if not issubclass(plugin.config_schema_cls, ma.Schema):
            raise Exception(
                'Config schema for plugin {} is not a marshmallow Schema. '
                'Found: {}'.format(plugin.name, plugin.config_schema_cls))

        parsed_config = plugin.config_schema_cls().load(config or {})
        if parsed_config.errors:
            raise Exception(
                'Errors parsing configuration for plugin {}: {}'.format(
                    plugin.name,
                    parsed_config.errors))

        return parsed_config.data

    def insert_imports(self, plugin_config):
        objects = []
        modules = []

        plugin_config = plugin_config or {}
        for plugin in self._plugins:
            conf = self.parse_plugin_config(plugin, plugin_config.get(plugin.name))
            plugin_imports = plugin.insert_imports(conf) or {}

            objects += plugin_imports.get('objects', [])
            modules += plugin_imports.get('modules', [])

        result = {}
        if objects:
            result['objects'] = objects

        if modules:
            result['modules'] = modules

        return result

    def _build_insert_dict(self, plugin_config, method):
        result = {}
        plugin_config = plugin_config or {}
        for plugin in self._plugins:
            conf = self.parse_plugin_config(plugin, plugin_config.get(plugin.name))
            plugin_items = method(plugin)(conf)

            result.update(plugin_items or {})

        return result

    def insert_default_task_args(self, plugin_config):
        return self._build_insert_dict(
            plugin_config,
            operator.attrgetter('insert_default_task_args'))

    def insert_dag_args(self, plugin_config):
        return self._build_insert_dict(
            plugin_config,
            operator.attrgetter('insert_dag_args'))

    def _build_insert_list(self, plugin_config, method):
        plugin_config = plugin_config or {}
        items = []
        for plugin in self._plugins:
            conf = self.parse_plugin_config(plugin, plugin_config.get(plugin.name))
            plugin_items = method(plugin)(conf)

            items += (plugin_items or [])

        return items

    def insert_before(self, plugin_config):
        return self._build_insert_list(plugin_config, operator.attrgetter('insert_before'))

    def insert_operators(self, plugin_config):
        return self._build_insert_list(plugin_config, operator.attrgetter('insert_operators'))

    def insert_after(self, plugin_config):
        return self._build_insert_list(plugin_config, operator.attrgetter('insert_after'))

    def operators(self, item):
        return self._registry_lookup(item, 'operator')

    def generators(self, item):
        return self._registry_lookup(item, 'generator')

    def subdags(self, item):
        return self._registry_lookup(item, 'subdag')

    def resources(self, item):
        return self._registry_lookup(item, 'resource')

    def property_preprocessors(self, item):
        return self._registry_lookup(item, 'property_preprocessor')

    def get_oozie_config(self, args):
        return OozieMetaPlugin(self._plugins, args)

    def _get_oozie_plugin_classes(self):
        return {
            plugin.name: plugin.oozie_plugin_cls for plugin in self._plugins
            if plugin.oozie_plugin_cls
        }

    def register_oozie_parser_arguments(self, parser):
        for cls in self._get_oozie_plugin_classes().values():
            cls.register_arguments(parser)

    def _load_package_plugins(self):
        return [entry_point.load()() for entry_point in
                pkg_resources.iter_entry_points(self.ENTRY_POINT_GROUP)]

    def _registry_lookup(self, item, registry_name):
        registry_getter = operator.attrgetter(registry_name + '_registry')

        prioritized_hits = {}
        for plugin in self._plugins:
            registry = registry_getter(plugin)
            if not registry:
                continue

            hit = registry.get(item)
            if not hit:
                continue

            prioritized_hits.setdefault(plugin.priority, [])
            prioritized_hits[plugin.priority].append(hit)

        if not prioritized_hits:
            available_types = frozenset(
                node_type for plugin in self._plugins
                if registry_getter(plugin)
                for node_type in registry_getter(plugin).node_configs)
            raise Exception(
                'Lookup failed for registry `{}`: `{}` not found for item `{}`. '
                'Available item types are: [`{}`]'.format(
                    registry_name,
                    item['type'],
                    item,
                    '`, `'.join(available_types)))

        highest_hit_priority = max(
            prioritized_hits.keys(),
            key=operator.attrgetter('value'))

        if not highest_hit_priority:
            raise Exception('Unknown {}: {}'.format(registry_name, item))

        hits = prioritized_hits[highest_hit_priority]

        if len(hits) > 1:
            raise Exception(
                'Ambiguous {registry_name} specification: {item} matches on '
                'multiple known {registry_name}s for plugins with priority '
                '{priority}: {hits}'.format(
                    registry_name=registry_name,
                    item=item,
                    priority=highest_hit_priority,
                    hits=hits))

        return hits[0]
