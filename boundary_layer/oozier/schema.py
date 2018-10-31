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


class OozieBaseSchema(ma.Schema):
    singletons_to_lists = []

    @ma.pre_load
    def _convert_singletons_to_lists(self, data):
        if all(isinstance(data.get(key, []), list) for key in self.singletons_to_lists):
            return data

        copy = data.copy()

        for key in self.singletons_to_lists:
            if not isinstance(data.get(key, []), list):
                copy[key] = [data.get(key)]

        return copy


class OozieNamedObjectSchema(OozieBaseSchema):
    name = ma.fields.String(required=True, load_from='@name')


class OozieFlowControlSchema(OozieBaseSchema):
    to = ma.fields.String(required=True, load_from='@to')


class OozieForkStartSchema(OozieBaseSchema):
    start = ma.fields.String(required=True, load_from='@start')


class OozieForkSchema(OozieNamedObjectSchema):
    path = ma.fields.List(ma.fields.Nested(OozieForkStartSchema), missing=[])

    singletons_to_lists = ['path']

    @ma.post_load
    def add_base_operator(self, data):
        operator = {
            'name': data['name'],
            'type': 'flow_control',
        }

        copy = data.copy()
        copy['operator'] = operator
        return copy


class OozieJoinSchema(OozieNamedObjectSchema, OozieFlowControlSchema):
    @ma.post_load
    def add_base_operator(self, data):
        operator = {
            'name': data['name'],
            'type': 'flow_control',
        }

        copy = data.copy()
        copy['operator'] = operator
        return copy


class OozieActionSchema(OozieNamedObjectSchema):
    ok = ma.fields.Nested(OozieFlowControlSchema, required=True)
    error = ma.fields.Nested(OozieFlowControlSchema, required=True)

    def _get_action_builder(self, data):
        keyed_action_builders = {
            builder.key: builder
            for builder in self.context['oozie_plugin'].action_builders()
        }

        keys_present = [key for key in keyed_action_builders if data.get(key)]

        if not keys_present:
            raise ma.ValidationError(
                'No known action builders present (expected one of {{{}}}, found keys: {}'.format(
                    ', '.join(keyed_action_builders.keys()),
                    ', '.join(data.keys())))

        if len(keys_present) > 1:
            raise ma.ValidationError(
                'Only one action type may be specified (found {})'.format(
                    keys_present))

        return keyed_action_builders[keys_present[0]]

    @ma.validates_schema(pass_original=True)
    def one_action_type(self, _, original):
        """ Runs the validation checks to make sure that exactly one
            known action is present, and prints an error message based on
            the content of the original, unparsed input
        """
        self._get_action_builder(original)

    @ma.post_load(pass_original=True)
    def fetch_base_action(self, data, original):
        builder_cls = self._get_action_builder(original)

        return builder_cls(self.context, data, original[builder_cls.key])


class OozieKillSchema(OozieNamedObjectSchema):
    message = ma.fields.String()


class OozieCaseSchema(OozieFlowControlSchema):
    text = ma.fields.String(required=True, load_from='#text')


class OozieSwitchSchema(OozieBaseSchema):
    case = ma.fields.List(ma.fields.Nested(OozieCaseSchema))
    default = ma.fields.Nested(OozieFlowControlSchema, required=True)

    singletons_to_lists = ['case']


class OozieDecisionSchema(OozieNamedObjectSchema):
    switch = ma.fields.Nested(OozieSwitchSchema)


class OozieWorkflowAppSchema(OozieNamedObjectSchema):
    name = ma.fields.String(required=True, load_from='@name')
    action = ma.fields.List(ma.fields.Nested(OozieActionSchema), missing=[])
    join = ma.fields.List(ma.fields.Nested(OozieJoinSchema), missing=[])
    fork = ma.fields.List(ma.fields.Nested(OozieForkSchema), missing=[])
    decision = ma.fields.List(ma.fields.Nested(
        OozieDecisionSchema), missing=[])
    start = ma.fields.Nested(OozieFlowControlSchema, required=True)
    end = ma.fields.Nested(OozieNamedObjectSchema, required=True)
    kill = ma.fields.Nested(OozieKillSchema)

    singletons_to_lists = ['fork', 'join', 'action', 'decision']


class OozieWorkflowSchema(OozieBaseSchema):
    workflow_app = ma.fields.Nested(
        OozieWorkflowAppSchema, load_from='workflow-app', required=True)

    @ma.post_load
    def return_workflow(self, data):
        return data['workflow_app']
