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
import marshmallow as ma
import jinja2
from boundary_layer.registry.types.preprocessor import PropertyPreprocessor
from boundary_layer.schemas.base import StrictSchema
from boundary_layer.logger import logger


class DateStringToDatetime(PropertyPreprocessor):
    type = "date_string_to_datetime"

    def imports(self):
        return {'modules': ['datetime']}

    def process_arg(self, arg):
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


class SecondsToTimedelta(PropertyPreprocessor):
    type = "seconds_to_timedelta"

    def imports(self):
        return {'modules': ['datetime']}

    def process_arg(self, arg):
        delta = None

        try:
            delta = datetime.timedelta(seconds=arg)
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

    def process_arg(self, arg):
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
            rendered_arg = self.render_template(arg)
        except jinja2.exceptions.UndefinedError:
            logger.warning(
                'Could not render template `%s`; cannot verify that the argument '
                'matches the required pattern `%s`!',
                arg,
                regex.pattern)
            return arg

        if not regex.match(rendered_arg):
            raise Exception(
                'Invalid argument `{}`: does not match expected pattern `{}`'.format(
                    rendered_arg,
                    regex.pattern))

        # return the original arg, not the rendered arg, because we are not
        # actually transforming anything, just validating
        return arg

    @property
    def pattern(self):
        # Return the pattern configured in the properties, but make sure that
        # the pattern ends in a `$` to ensure that it applies to the entire
        # string, not a prefix of it

        pattern = self.properties['pattern'].rstrip('$')

        return pattern + '$'

    def render_template(self, arg):
        env = jinja2.Environment(undefined=jinja2.StrictUndefined)

        return env.from_string(arg).render(**self._template_context())

    @staticmethod
    def _template_context():
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
            'ts_nodash': now.isoformat().replace('-', '')
        }

        for field_name in ['ds', 'yesterday_ds', 'tomorrow_ds', 'next_ds', 'prev_ds']:
            context[field_name] = now.strftime('%Y-%m-%d')
            context[field_name + '_nodash'] = now.strftime('%Y%m%d')

        for field_name in ['execution_date', 'prev_execution_date', 'next_execution_date']:
            context[field_name] = now

        return context
