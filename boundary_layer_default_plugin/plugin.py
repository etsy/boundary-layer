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
from boundary_layer.plugins import BasePlugin, PluginPriority
from .oozie_plugin import DefaultOozieParserPlugin
from .preprocessors import DateStringToDatetime, BuildTimedelta, EnsureRenderedStringPattern
from .preprocessors import KubernetesPrep


class DefaultPlugin(BasePlugin):
    name = 'default'

    priority = PluginPriority.DEFAULT

    configs_path = os.path.join(os.path.dirname(__file__), 'config')

    oozie_plugin_cls = DefaultOozieParserPlugin

    property_preprocessors = [
        DateStringToDatetime,
        BuildTimedelta,
        EnsureRenderedStringPattern,
        KubernetesPrep]
