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

from jsonschema import Draft4Validator
import marshmallow as ma
from boundary_layer.logger import logger
from boundary_layer.schemas.base import StrictSchema
from boundary_layer.schemas.dag import ImportSchema


class JsonSchemaSchema(StrictSchema):
    properties = ma.fields.Dict()
    additionalProperties = ma.fields.Boolean()
    required = ma.fields.List(ma.fields.String())


class BaseSpecSchema(StrictSchema):
    name = ma.fields.String(required=True)
    imports = ma.fields.Nested(ImportSchema)
    parameters_jsonschema = ma.fields.Nested(JsonSchemaSchema)
    schema_extends = ma.fields.String()

    @ma.validates_schema
    def jsonschema_or_extended(self, data):
        if 'parameters_jsonschema' in data or 'schema_extends' in data:
            return

        raise ma.ValidationError(
            'No mechanism for determining properties schema for `{}`: Must provide '
            'at least one of (parameters_jsonschema, schema_extends)'.format(
                data.get('name', data)))

    @ma.validates_schema
    def check_jsonschema(self, data):
        if 'parameters_jsonschema' not in data:
            return
        # Make sure that `properties` is present, because it's not actually
        # required to make a valid JSONSchema
        if 'properties' not in data.get('parameters_jsonschema', {}):
            logger.debug(
                'No `properties` defined in `parameters_jsonschema` for `%s`',
                data['name'])
        try:
            Draft4Validator.check_schema(data['parameters_jsonschema'])
        except Exception as e:
            raise ma.ValidationError('Invalid JSON schema: {}'.format(e),
                                     ['parameters_jsonschema'])
