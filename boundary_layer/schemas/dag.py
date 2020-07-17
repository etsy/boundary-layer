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

import re
from datetime import timedelta
import semver
from marshmallow import fields, validates_schema, ValidationError, post_dump
from boundary_layer import VERSION, MIN_SUPPORTED_VERSION
from boundary_layer.schemas.base import StrictSchema


class OperatorSchema(StrictSchema):
    name = fields.String(required=True)
    type = fields.String(required=True)

    requires_resources = fields.List(fields.String())

    upstream_dependencies = fields.List(fields.String())
    downstream_dependencies = fields.List(fields.String())

    properties = fields.Dict()


class ReferenceSchema(OperatorSchema):
    target = fields.String(required=True)


class GeneratorSchema(ReferenceSchema):
    auto_task_id_mode = fields.String()
    regex_blocklist = fields.List(fields.String())

    @validates_schema
    def check_task_id_mode(self, data):
        if 'auto_task_id_mode' not in data:
            return

        allowed_values = ('index', 'item_name')
        if data['auto_task_id_mode'] not in allowed_values:
            raise ValidationError(
                'Value must be one of {}'.format(allowed_values),
                ['auto_task_id_mode'])


class ResourceSchema(OperatorSchema):
    # Resources are like operators, but they must always have both a
    # create step and a destroy step.  An example is a transient cluster
    # on DataProc / EMR.  These will generally be specified
    # by the class referenced by the `type` argument, although we
    # should also permit a 'custom' type which specifies create/destroy
    # arguments in the config directly

    # An additional optional parameter allows the DAG-writer to disable
    # the creation of sentinel nodes for cases in which the destroy
    # operator is a leaf node.
    disable_sentinel_node = fields.Boolean()


class ObjectImportSchema(StrictSchema):
    module = fields.String(required=True)
    objects = fields.List(fields.String(), required=True)


class ImportSchema(StrictSchema):
    modules = fields.List(fields.String())
    objects = fields.List(fields.Nested(ObjectImportSchema))


class BaseDagSchema(StrictSchema):
    name = fields.String(required=True)

    imports = fields.Nested(ImportSchema)

    resources = fields.List(fields.Nested(ResourceSchema))

    before = fields.List(fields.Nested(OperatorSchema))
    operators = fields.List(fields.Nested(OperatorSchema))
    after = fields.List(fields.Nested(OperatorSchema))

    # we treat sub-dags and generators as a special first-class category of
    # Reference operators.
    sub_dags = fields.List(fields.Nested(ReferenceSchema()))
    generators = fields.List(fields.Nested(GeneratorSchema()))

    plugin_config = fields.Dict()

    @validates_schema
    def validate_plugin_config(self, data):
        from boundary_layer import plugins
        if 'plugin_config' not in data:
            return

        try:
            plugins.manager.validate_config(data['plugin_config'])
        except ValidationError as e:
            raise e
        except Exception as e:
            raise ValidationError(str(e))


class DagArgsSchema(StrictSchema):
    catchup = fields.Boolean(missing=True)
    max_active_runs = fields.Integer(missing=1)
    concurrency = fields.Integer()
    # schedule_interval argument supports cron strings (e.g. 0 * * * *),
    # '@hourly/daily/etc', or numeric (seconds)
    schedule_interval = fields.String(allow_none=True)
    description = fields.String()
    dagrun_timeout = fields.Integer()
    default_view = fields.String()
    orientation = fields.String()
    sla_miss_callback = fields.String()
    on_success_callback = fields.String()
    on_failure_callback = fields.String()
    params = fields.Dict()
    full_filepath = fields.String()
    template_searchpath = fields.List(fields.String())
    template_undefined = fields.String()
    user_defined_macros = fields.Dict()
    user_defined_filters = fields.Dict()
    doc_md = fields.String()
    access_control = fields.Dict()

    @validates_schema
    def validate_callbacks(self, data):
        callbacks = ['sla_miss_callback', 'on_success_callback',
                     'on_failure_callback']
        for cb in callbacks:
            if cb not in data:
                continue
            if not re.compile('<<.+>>').match(data[cb]):
                raise ValidationError(
                    '{} must be a verbatim string like <<...>>'.format(cb),
                    [cb])

    @validates_schema
    def validate_default_view(self, data):
        if 'default_view' not in data:
            return

        allowed_values = ('tree', 'graph', 'duration', 'gantt',
                          'landing_times')
        if data['default_view'] not in allowed_values:
            raise ValidationError(
                'Value must be one of {}'.format(allowed_values),
                ['default_view'])

    @validates_schema
    def validate_orientation(self, data):
        if 'orientation' not in data:
            return

        allowed_values = ('LR', 'TB', 'RL', 'BT')
        if data['orientation'] not in allowed_values:
            raise ValidationError(
                'Value must be one of {}'.format(allowed_values),
                ['orientation'])

    @validates_schema
    def validate_template_undefined(self, data):
        if 'template_undefined' not in data:
            return
        if not re.compile('<<.+>>').match(data['template_undefined']):
            raise ValidationError(
                'template_undefined must be a verbatim string like <<...>>',
                ['template_undefined'])

    @post_dump
    def dagrun_timeout_to_timedelta(self, data):
        if not self.context.get('for_dag_output'):
            return data
        if 'dagrun_timeout' in data:
            try:
                delta = timedelta(seconds=data['dagrun_timeout'])
            except TypeError as e:
                raise Exception(
                    'Error in making dagrun_timeout into timedelta object : {}'
                    .format(str(e)))
            data['dagrun_timeout'] = delta
        return data


class PrimaryDagSchema(BaseDagSchema):
    compatibility_version = fields.String()

    dag_args = fields.Nested(DagArgsSchema)

    default_task_args = fields.Dict()

    @validates_schema
    def validate_compatibility_version(self, data):
        if not data.get('compatibility_version'):
            return

        version = None
        try:
            version = semver.parse_version_info(data['compatibility_version'])
        except ValueError:
            raise ValidationError('Must be a valid SemVer',
                                  ['compatibility_version'])

        if VERSION < version:
            raise ValidationError(
                'Incompatible boundary_layer version: This '
                'workflow requires boundary_layer version {} or higher! '
                'Current version is {}'.format(version, VERSION),
                ['compatibility_version'])

        if version < MIN_SUPPORTED_VERSION:
            raise ValidationError(
                'Incompatible boundary_layer version: This workflow '
                'is for the incompatible prior version {}. Use the '
                'migrate-workflow script to update it.'.format(version),
                ['compatibility_version'])
