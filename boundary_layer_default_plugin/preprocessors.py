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
import datetime
from collections import namedtuple
import json
import base64
import marshmallow as ma
import jinja2
from boundary_layer.registry.types.preprocessor import PropertyPreprocessor
from boundary_layer.schemas.base import StrictSchema
from boundary_layer.logger import logger
from boundary_layer.util import GenericNamedParameterPasser


class DateStringToDatetime(PropertyPreprocessor):
    type = "date_string_to_datetime"

    def imports(self):
        return {'modules': ['datetime']}

    def process_arg(self, arg, node, raw_args):
        date = None
        try:
            date = datetime.datetime.strptime(arg, '%Y-%m-%d')
        except ValueError as e:
            raise Exception(
                'Error in preprocessor {} for argument `{}`: {}'.format(
                    self.type,
                    arg,
                    str(e)))

        return date


class BuildKubernetesSchema(StrictSchema):
    class_name = ma.fields.String(required=True)

    @ma.validates_schema
    def check_valid_class(self, data):
        ALLOWED_CLASS = ['airflow.contrib.kubernetes.volume.Volume',
                         'airflow.contrib.kubernetes.volume_mount.VolumeMount',
                         'airflow.contrib.kubernetes.secret.Secret',
                         'airflow.contrib.kubernetes.pod.Resources']
        if data.get('class_name') not in ALLOWED_CLASS:
            raise ma.ValidationError(
                '`class_name` must be one of `{}`'.format(
                    '`, `'.join(ALLOWED_CLASS)),
                ['class_name'])


class KubernetesPrep(PropertyPreprocessor):
    type = "kubernetes_prep"
    properties_schema_cls = BuildKubernetesSchema

    def imports(self):
        return {'modules': ['.'.join(self.properties['class_name'].split('.')[:-1])]}

    def process_arg(self, arg, node, raw_args):
        kube_objects = None
        try:
            if isinstance(arg, list):
                kube_objects = [GenericNamedParameterPasser(self.properties['class_name'], a)
                                for a in arg]
            else:
                kube_objects = GenericNamedParameterPasser(self.properties['class_name'], arg)
        except Exception as e:
            raise Exception(
                'Error in preprocessor {} for argument `{}`: {}'.format(
                    self.type,
                    arg,
                    str(e)))

        return kube_objects


class BuildTimedeltaSchema(StrictSchema):
    units = ma.fields.String(required=True)

    @ma.validates_schema
    def check_valid_units(self, data):
        ALLOWED_UNITS = ['seconds', 'minutes', 'hours', 'days']
        if data.get('units') not in ALLOWED_UNITS:
            raise ma.ValidationError(
                '`units` must be one of `{}`'.format(
                    '`, `'.join(ALLOWED_UNITS)),
                ['units'])


class BuildTimedelta(PropertyPreprocessor):
    type = "to_timedelta"

    properties_schema_cls = BuildTimedeltaSchema

    def imports(self):
        return {'modules': ['datetime']}

    def process_arg(self, arg, node, raw_args):
        delta = None

        try:
            timedelta_arg = {self.properties['units']: arg}
            delta = datetime.timedelta(**timedelta_arg)
        except TypeError as e:
            raise Exception(
                'Error in preprocessor {} for argument `{}`: {}'.format(
                    self.type,
                    arg,
                    str(e)))

        return delta


class EnsureRenderedStringPatternSchema(StrictSchema):
    pattern = ma.fields.String(required=True)


class EnsureRenderedStringPattern(PropertyPreprocessor):
    type = "ensure_rendered_string_pattern"

    properties_schema_cls = EnsureRenderedStringPatternSchema

    def process_arg(self, arg, node, raw_args):
        regex = None
        try:
            regex = re.compile(self.pattern)
        except Exception:
            raise Exception(
                'Error compiling regex for `{}`: `{}` is an invalid pattern'.format(
                    self.type,
                    self.properties['pattern']))

        rendered_arg = None
        try:
            rendered_arg = self.render_template(arg, raw_args)
        except jinja2.exceptions.UndefinedError:
            logger.warning(
                'Could not render template `%s`; cannot verify that the argument '
                'matches the required pattern `%s`!',
                arg,
                regex.pattern)
            return arg

        if regex.match(rendered_arg):
            # return the original arg, not the rendered arg, because we are not
            # actually transforming anything, just validating
            return arg

        VERBATIM_REGEX = '<<.+>>'
        if re.compile(VERBATIM_REGEX).search(rendered_arg):
            logger.warning(
                'Argument generated from `%s` may not match the required pattern `%s` and fail.',
                rendered_arg,
                regex.pattern)
            return arg

        raise Exception(
            'Invalid argument `{}`: does not match expected pattern `{}`'.format(
                rendered_arg,
                regex.pattern))

    @property
    def pattern(self):
        # Return the pattern configured in the properties, but make sure that
        # the pattern ends in a `$` to ensure that it applies to the entire
        # string, not a prefix of it

        pattern = self.properties['pattern'].rstrip('$')

        return pattern + '$'

    def render_template(self, arg, raw_args):
        env = jinja2.Environment(undefined=jinja2.StrictUndefined)

        return env.from_string(arg).render(**self._template_context(raw_args))

    @staticmethod
    def _template_context(raw_args):
        """ Create an emulated jinja context for common airflow date-related
            macros. The actual values of the macros do not matter, we are only
            concerned with general formatting which will be properly represented
            for any date.

            :returns: the context
            :rtype: dict
        """

        now = datetime.datetime.utcnow()

        context = {
            'ts': now.isoformat(),
            'ts_nodash': now.isoformat().replace('-', ''),
        }

        TaskTuple = namedtuple('TaskTuple', ['task_id'])
        task_id = raw_args.get('task_id')
        if task_id:
            context['task'] = TaskTuple(task_id=task_id)

        for field_name in ['ds', 'yesterday_ds', 'tomorrow_ds', 'next_ds', 'prev_ds']:
            context[field_name] = now.strftime('%Y-%m-%d')
            context[field_name + '_nodash'] = now.strftime('%Y%m%d')

        for field_name in ['execution_date', 'prev_execution_date', 'next_execution_date']:
            context[field_name] = now

        return context

class PubsubMessageDataToBinaryString(PropertyPreprocessor):
    """
    Converts pubsub message data with various python types 
    to binary strings.
    Supported message data arg types: `dict`, and `str`

    If arg is type `dict` that data will be converted
    into a json string, and then encoded into a binary string

    This preprocessing is necessary as messages with the `data`
    property are required to be binary strings to use airflow's
    provided pubsub functionality
    """
    type = "pubsub_message_data_to_binary_string"

    def imports(self):
        return {'modules': ['json', 'base64']}

    def process_arg(self, arg, node, raw_args):
        """
        Given array of messages, if `data` property is present in
        message objects, convert to binary string
        """
        res = []
        for message in arg:
            if 'data' in message:
                message['data'] = self._process_data_arg(message['data'])
            res.append(message)
        return res

    def _process_data_arg(self, arg):
        """
        Given either a dict or str:
        - If str, encode to binary string
        - If dict, convert to json and encode as binary string
        """
        bin_string = None
        try:
            res_str = arg if not self._is_dict(arg) else self._json_handler(arg)
            bin_string = base64.b64encode(res_str.encode('utf-8'))
        except Exception as e:
            raise Exception(
                'Error in preprocessor {} for argument `{}`: {}'.format(
                    self.type,
                    arg,
                    str(e)))
        return bin_string

    def _json_handler(self, arg):
        return json.dumps(arg)

    def _is_dict(self, arg):
        return isinstance(arg, dict)
