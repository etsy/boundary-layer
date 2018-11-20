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

from boundary_layer.registry import ConfigFileRegistry, NodeTypes
from boundary_layer.schemas.internal.generators import GeneratorSpecSchema
from boundary_layer.registry.types.subdag import SubdagNode


class GeneratorNode(SubdagNode):
    type = NodeTypes.GENERATOR

    operator_class = None
    operator_class_module = None

    @property
    def regex_blocklist(self):
        return self.item.get('regex_blocklist', ())


class GeneratorRegistry(ConfigFileRegistry):
    node_cls = GeneratorNode
    spec_schema_cls = GeneratorSpecSchema
