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
import marshmallow as ma
from boundary_layer.schemas.internal.base import BaseSpecSchema
from boundary_layer.schemas.base import StrictSchema


class PropertyPreprocessorSchema(StrictSchema):
    type = ma.fields.String(required=True)
    properties = ma.fields.Dict()
    apply_to_properties = ma.fields.List(ma.fields.String, required=True)


class OperatorSpecSchema(BaseSpecSchema):
    operator_class = ma.fields.String(required=True)
    operator_class_module = ma.fields.String(required=True)

    property_preprocessors = ma.fields.List(ma.fields.Nested(PropertyPreprocessorSchema))

    @ma.validates_schema
    def valid_preprocessor_property_names(self, data):
        preprocessors = data.get('property_preprocessors', [])

        if not preprocessors:
            return

        properties = frozenset(data.get('parameters_jsonschema', {}).get('properties', []))

        for preprocessor in preprocessors:
            missing = [
                property_name for property_name in preprocessor['apply_to_properties']
                if property_name not in properties]

            if missing:
                raise ma.ValidationError(
                    'Properties specified by preprocessor `{}` are not present '
                    'in the schema: `{}`'.format(
                        preprocessor['type'],
                        '`, `'.join(missing)))

        assigned_preprocessors = {
            property_name: [
                preprocessor['type'] for preprocessor in preprocessors
                if property_name in preprocessor['apply_to_properties']]
            for property_name in properties
        }

        dupes = [
            key for (key, value) in six.iteritems(assigned_preprocessors)
            if len(value) > 1]

        if dupes:
            raise ma.ValidationError(
                'One or more properties were assigned multiple preprocessors. '
                'This is not permitted.  Found: {}'.format({
                    key: value for (key, value) in six.iteritems(assigned_preprocessors)
                    if len(value) > 1
                }),
                ['property_preprocessors'])
