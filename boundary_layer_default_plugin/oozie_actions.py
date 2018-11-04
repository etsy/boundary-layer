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

import marshmallow as ma
from boundary_layer.oozier.schema import OozieBaseSchema
from boundary_layer.oozier.actions import OozieActionBuilderWithSchema, OozieSubWorkflowBuilderBase


class OozieSubWorkflowBuilder(OozieSubWorkflowBuilderBase):
    key = 'sub-workflow'


class OozieFileSystemSourceDestSchema(OozieBaseSchema):
    source = ma.fields.String(required=True)
    target = ma.fields.String(required=True)


class OozieFileSystemPathSchema(OozieBaseSchema):
    path = ma.fields.String(required=True, load_from='@path')


class OozieFileSystemActionSchema(OozieBaseSchema):
    delete = ma.fields.List(ma.fields.Nested(OozieFileSystemPathSchema))
    mkdir = ma.fields.List(ma.fields.Nested(OozieFileSystemPathSchema))
    move = ma.fields.List(ma.fields.Nested(OozieFileSystemSourceDestSchema))

    singletons_to_lists = ['delete', 'mkdir', 'move']


class OozieFileSystemActionBuilder(OozieActionBuilderWithSchema):
    key = 'fs'
    schema = OozieFileSystemActionSchema

    def get_operator(self):
        return {
            'name': self.name,
            'type': 'file_system',
            'properties': {}
        }


class OozieSshActionSchema(OozieBaseSchema):
    args = ma.fields.List(ma.fields.String(), missing=[])
    capture_output = ma.fields.Boolean(allow_none=True)
    command = ma.fields.String(required=True)
    host = ma.fields.String(required=True)


class OozieSshActionBuilder(OozieActionBuilderWithSchema):
    key = 'ssh'
    schema = OozieSshActionSchema

    def get_operator(self):
        properties = {
            'remote_host': self.data['host'],
            'command': self.data['command'],
        }
        return {
            'name': self.name,
            'type': 'ssh',
            'properties': properties,
        }


class OozieMapReduceActionSchema(OozieBaseSchema):
    arguments = ma.fields.List(ma.fields.String(), missing=[])


class OozieMapReduceActionBuilder(OozieActionBuilderWithSchema):
    key = 'map-reduce'
    schema = OozieMapReduceActionSchema

    def get_operator(self):
        cluster_config = self.context['cluster_config']
        operator = {
            'name': self.name,
            'type': cluster_config.mapreduce_operator_type,
            'properties': self.data,
        }

        if cluster_config.managed_resource:
            operator['requires_resources'] = [
                cluster_config.managed_resource['name']
            ]

        return operator
