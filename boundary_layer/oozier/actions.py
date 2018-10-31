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
import marshmallow as ma
from boundary_layer.oozier.schema import OozieBaseSchema


class OozieActionBuilder(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, context, action_metadata, data):
        self.context = context
        self.action_metadata = action_metadata
        self.data = data

    @property
    def ok_to(self):
        return self.action_metadata['ok']['to']

    @property
    def name(self):
        return self.action_metadata['name']

    @abc.abstractproperty
    def key(self):
        pass

    @abc.abstractmethod
    def get_action(self):
        pass

    @abc.abstractmethod
    def get_operator(self):
        pass


class OozieActionBuilderWithSchema(OozieActionBuilder):
    @abc.abstractproperty
    def schema(self):
        pass

    def __init__(self, context, action_metadata, data):
        loaded = self.schema(context=context).load(data)
        if loaded.errors:
            raise ma.ValidationError(loaded.errors)

        super(OozieActionBuilderWithSchema, self).__init__(context, action_metadata, loaded.data)

    def get_action(self):
        result = self.action_metadata.copy()
        result.update(self.data)

        return result


class OozieSubWorkflowActionSchema(OozieBaseSchema):
    app_path = ma.fields.String(required=True, load_from='app-path')
    propagate_configuration = ma.fields.Dict(allow_none=True)


class OozieSubWorkflowBuilderBase(OozieActionBuilderWithSchema):
    schema = OozieSubWorkflowActionSchema

    def __init__(self, context, action_metadata, data):
        super(OozieSubWorkflowBuilderBase, self).__init__(
            context, action_metadata, data)

        # We generally do not know the name of the target sub-workflow
        # a priori; we must load it to find out.  We store the name in this
        # class, and insert it inside the parser using the setter method,
        # below.
        self._target_sub_workflow_name = None

    def set_target_name(self, name):
        self._target_sub_workflow_name = name

    @property
    def target_name(self):
        if not self._target_sub_workflow_name:
            raise Exception(
                'Cannot get sub_workflow_name for action {}: not yet set!'.format(
                    self.get_action()))

        return self._target_sub_workflow_name

    @abc.abstractproperty
    def key(self):
        pass

    def get_operator(self):
        return {
            'name': self.name,
            'type': 'sub_dag',
            'target': self.target_name,
        }
