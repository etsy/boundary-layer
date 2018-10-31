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

from boundary_layer.registry import ConfigFileRegistry, RegistryNode, NodeTypes
from boundary_layer.schemas.internal.resources import ResourceSpecSchema


class ResourceNode(RegistryNode):
    type = NodeTypes.RESOURCE

    def imports(self):
        create = self.create_operator.imports()
        destroy = self.destroy_operator.imports()

        return {
            'modules': create['modules'] + destroy['modules'],
            'objects': create['objects'] + destroy['objects'],
        }

    def __init__(self, config, item):
        super(ResourceNode, self).__init__(config, item)
        self._create_operator_node = None
        self._destroy_operator_node = None

    @property
    def create_operator_type(self):
        return self.config['create_operator_type']

    @property
    def create_operator(self):
        if not self._create_operator_node:
            raise Exception(
                'resolve_properties() has not yet been called on {}'.format(
                    self))
        return self._create_operator_node

    @property
    def destroy_operator_type(self):
        return self.config.get('destroy_operator_type')

    @property
    def disable_sentinel_node(self):
        """ Determine whether the DAG or the resource config specifies that
            we should disable the sentinel node (or not).  Note that we allow
            the DAG to override any configuration that may be present in the
            resource config, rather than vice-versa.
        """
        return self.item.get(
            'disable_sentinel_node',
            self.config.get('disable_sentinel_node', False))

    @property
    def destroy_operator(self):
        if not self.destroy_operator_type:
            return None

        if not self._destroy_operator_node:
            raise Exception(
                'resolve_properties() has not yet been called on {}'.format(
                    self))
        return self._destroy_operator_node

    @property
    def provides_args(self):
        return self.config['provides_args']

    def resolve_properties(
            self,
            execution_context,
            default_task_args,
            base_operator_loader,
            preprocessor_loader):

        self._create_operator_node = base_operator_loader({
            'name': self.name + '_create',
            'type': self.create_operator_type,
            'properties': self.item.get('properties', {}),
            'upstream_dependencies': self.upstream_dependencies,
            'downstream_dependencies': self.downstream_dependencies,
            'requires_resources': self.requires_resources,
        })

        create_properties = self._create_operator_node.resolve_properties(
            execution_context=execution_context,
            default_task_args=default_task_args,
            base_operator_loader=base_operator_loader,
            preprocessor_loader=preprocessor_loader)

        if not self.destroy_operator_type:
            return (create_properties, None)

        self._destroy_operator_node = base_operator_loader({
            'name': self.name + '_destroy',
            'type': self.destroy_operator_type,
            'properties': {},
            'requires_resources': [self.name]
        })

        resource_context_with_self = execution_context.resources.copy()
        resource_context_with_self[self.name] = self
        destroy_properties = self._destroy_operator_node.resolve_properties(
            execution_context=execution_context._replace(resources=resource_context_with_self),
            default_task_args=default_task_args,
            base_operator_loader=base_operator_loader,
            preprocessor_loader=preprocessor_loader)

        return (create_properties, destroy_properties)

    def get_provided_args(self):
        create_operator_properties = self.create_operator.resolved_properties
        return {
            name: create_operator_properties.values.get(name)
            for name in self.provides_args
        }


class ResourceRegistry(ConfigFileRegistry):
    spec_schema_cls = ResourceSpecSchema

    node_cls = ResourceNode
