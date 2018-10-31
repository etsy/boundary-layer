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
from boundary_layer.plugins import util


class OozieMetaPlugin(object):
    def __init__(self, oozie_plugin_classes, args):
        self._plugin_classes = {
            key: value for (key, value) in six.iteritems(oozie_plugin_classes)
            if value
        }

        self.args = args

        self._plugins = {
            name: cls(args) for (name, cls) in six.iteritems(self._plugin_classes)
        }

    def action_builders(self):
        return util.merge_lists(
            plugin.action_builders() for plugin in self._plugins.values())

    def plugin_config(self):
        all_configs = {
            plugin_name: plugin.plugin_config() for (plugin_name, plugin)
            in six.iteritems(self._plugins)
        }

        return {name: config for (name, config) in six.iteritems(all_configs) if config}

    def dag_args(self):
        return util.merge_dicts(
            plugin.dag_args() for plugin in self._plugins.values())

    def default_task_args(self):
        return util.merge_dicts(
            plugin.default_task_args() for plugin in self._plugins.values())

    def dag_imports(self):
        return util.merge_dicts(
            plugin.dag_imports() for plugin in self._plugins.values())

    def cluster_config(self):
        return util.merge_dicts(
            plugin.cluster_config() for plugin in self._plugins.values())

    def upstream_operators(self):
        return util.merge_lists(
            plugin.upstream_operators() for plugin in self._plugins.values())

    def jsp_macros(self):
        return util.merge_dicts(
            plugin.jsp_macros() for plugin in self._plugins.values())


class BaseOozieParserPlugin(object):
    def action_builders(self):
        """ Plugin may provide customized parsers for oozie actions.
            Must be a list of OozieActionBuilder classes
        """
        return []

    def plugin_config(self):
        return {}

    def dag_args(self):
        return {}

    def default_task_args(self):
        return {}

    def cluster_config(self):
        return {}

    def dag_imports(self):
        return {}

    def upstream_operators(self):
        return []

    def jsp_macros(self):
        return {}

    @classmethod
    def register_arguments(cls, parser):
        # Overrideable method for adding command-line arguments
        pass

    def __init__(self, args):
        self.args = args
