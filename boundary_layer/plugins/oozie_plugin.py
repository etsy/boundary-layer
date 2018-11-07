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

from collections import namedtuple
import six
from boundary_layer.plugins import util
from boundary_layer.logger import logger


OoziePluginContainer = namedtuple('OoziePluginContainer', ['name', 'priority', 'plugin'])


class OozieMetaPlugin(object):
    def __init__(self, plugins, args):
        self.args = args

        sorted_plugins = sorted(
            plugins,
            key=lambda p: p.priority.value,
            reverse=True)

        self._plugin_containers = [
            OoziePluginContainer(
                name=plugin.name,
                priority=plugin.priority,
                plugin=plugin.oozie_plugin_cls(args)) for plugin in sorted_plugins]

    def action_builders(self):
        return util.merge_lists(
            pc.plugin.action_builders() for pc in self._plugin_containers)

    def plugin_config(self):
        all_configs = {
            plugin_container.name: plugin_container.plugin.plugin_config()
            for plugin_container in self._plugin_containers
        }

        return {name: config for (name, config) in six.iteritems(all_configs) if config}

    def dag_args(self):
        return util.merge_dicts(
            pc.plugin.dag_args() for pc in self._plugin_containers)

    def default_task_args(self):
        return util.merge_dicts(
            pc.plugin.default_task_args() for pc in self._plugin_containers)

    def dag_imports(self):
        return util.merge_dicts(
            pc.plugin.dag_imports() for pc in self._plugin_containers)

    def cluster_config(self):
        plugins_with_cluster_configs = [
            pc for pc in self._plugin_containers
            if pc.plugin.cluster_config()]

        if not plugins_with_cluster_configs:
            raise Exception('No cluster configurations found for oozie parser!')

        if len(plugins_with_cluster_configs) > 1:
            logger.info(
                'Multiple cluster configurations found.  Choosing configuration '
                'from plugin `%s`, with priority `%s`',
                plugins_with_cluster_configs[0].name,
                plugins_with_cluster_configs[0].priority)

        return plugins_with_cluster_configs[0].plugin.cluster_config()

    def upstream_operators(self):
        return util.merge_lists(
            pc.plugin.upstream_operators() for pc in self._plugin_containers)

    def jsp_macros(self):
        return util.merge_dicts(
            pc.plugin.jsp_macros() for pc in self._plugin_containers)


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
        return None

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
