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
            'command': ' '.join([self.data['command']] + self.data['args']),
        }
        return {
            'name': self.name,
            'type': 'ssh',
            'properties': properties,
        }


class OozieNameValueSchema(OozieBaseSchema):
    name = ma.fields.String(required=True)
    value = ma.fields.String(required=True)


class OozieHadoopConfigurationSchema(OozieBaseSchema):
    _property = ma.fields.List(
        ma.fields.Nested(OozieNameValueSchema),
        required=True,
        load_from='property',
        dump_to='property',
        attribute='property')

    singletons_to_lists = ['property']


class OozieMapReduceActionSchema(OozieBaseSchema):
    arg = ma.fields.List(ma.fields.String(), missing=[])
    configuration = ma.fields.Nested(OozieHadoopConfigurationSchema)
    job_tracker = ma.fields.String(required=True, load_from='job-tracker')
    name_node = ma.fields.String(required=True, load_from='name-node')
    main_class = ma.fields.String(load_from='main-class')


class OozieMapReduceActionBuilder(OozieActionBuilderWithSchema):
    key = 'map-reduce'
    schema = OozieMapReduceActionSchema

    discard_property_keys = []

    def translated_args(self):
        return self.context['macro_translator'].translate(self.data['arg'])

    def operator_properties(self):
        properties = {'arguments': self.translated_args()}

        if self.data.get('main_class'):
            properties['main_class'] = self.data['main_class']

        config_properties = {}
        if self.data.get('configuration', {}).get('property'):
            config_properties = self.context['macro_translator'].translate({
                item['name']: item['value']
                for item in self.data['configuration']['property']
                if item['name'] not in self.discard_property_keys
            })

            if config_properties:
                properties['dataproc_hadoop_properties'] = config_properties

        cluster_config = self.context['cluster_config']
        return cluster_config.apply_config_properties(properties, config_properties)

    def get_operator(self):
        cluster_config = self.context['cluster_config']
        operator = {
            'name': self.name,
            'type': cluster_config.mapreduce_operator_type,
            'properties': self.operator_properties(),
        }

        if cluster_config.managed_resource:
            operator['requires_resources'] = [
                cluster_config.managed_resource['name']
            ]

        return operator
