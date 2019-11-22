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

import datetime
import six
from six.moves import filter, map
from boundary_layer.util import sanitize_operator_name
from boundary_layer.util import GenericNamedParameterPasser


def verbatim(item):
    if isinstance(item, list):
        return list(map(verbatim, item))
    elif isinstance(item, six.string_types):
        return '<<' + item + '>>'

    raise Exception('Cannot cast item `{}` to a verbatim string!'.format(
        item))


def construct_dag_name(reference_path):
    return '.'.join(map(sanitize_operator_name, reference_path))


def subdag_builder_name(sub_workflow_name):
    return '{}_builder'.format(sanitize_operator_name(sub_workflow_name))


def add_leading_spaces(text_block, indent_depth):
    indent_spaces = 4
    num_spaces = indent_spaces * indent_depth
    return '\n'.join((' ' * num_spaces + line) for line in text_block.split('\n'))


def enquote(x):
    if "'" not in x:
        return "'%s'" % x

    if '"' not in x:
        return '"%s"' % x

    assert '"""' not in x, 'Cannot wrap quotes around string: {}'.format(x)

    return '"""%s"""' % x


def split_verbatim(value):
    """ Method for handling verbatim indicator characters in strings, which
        are used to indicate that the string should be inserted verbatim into
        the dag as python code, rather than as a string. These are used,
        for example, to allow us to write simple templates for the "generator"
        actions, which construct a for-loop and have to refer to the loop
        variable.  We use the delimiters `<<` and `>>` to indicate verbatim
        strings.  I hope nobody wants to use the left- and right-shift
        inside one of these things...
    """
    def wrap(item, in_block):
        """ non-verbatim strings must be wrapped in quotes; we wrap verbatim
            strings in parentheses in order to ensure proper order-of-operations
            when we join together the components with '+'
        """
        if not item:
            return ''

        return enquote(item) if not in_block else '({})'.format(item)

    def _do_split(remaining, in_block):
        if not in_block and '<<' not in remaining:
            return [wrap(remaining, in_block)]

        if in_block and '>>' not in remaining:
            raise Exception(
                'Unpaired verbatim string character `<<` in string {}'.format(value))

        idx_start = remaining.index('<<' if not in_block else '>>')

        if in_block and '<<' in remaining and remaining.index('<<') < idx_start:
            raise Exception(
                'Invalid nested verbatim string characters in string {}'.format(value))

        before_slice = remaining[:idx_start]
        after_slice = remaining[(2+idx_start):]

        return [wrap(before_slice, in_block)] + \
            _do_split(after_slice, not in_block)

    return list(filter(None, _do_split(remaining=value, in_block=False)))


def format_value(value):
    """ Format values for passing arguments to airflow operators.  This
        involves differentiating between string components that should be
        inserted verbatim (e.g. variable references) from those that should be
        quoted.  Our convention for verbatim strings is that they are inclosed in
        delimiters << >>.  For example, the following string:
            "<< '10' if some_value else '20' >> dags"
        will be transformed to the python code:
            ('10' if some_value else '20') + 'dags'
        This enables the yaml dags specifications to contain arbitrary
        logical statements.
    """
    if isinstance(value, list):
        return '[{}]'.format(','.join(map(format_value, value)))

    if isinstance(value, dict):
        pairs = ['{}: {}'.format(enquote(k), format_value(v))
                 for (k, v) in value.items()]

        return '{{ {items} }}'.format(items=','.join(pairs))

    if isinstance(value, (int, float, type(None))):
        return str(value)

    if isinstance(value, (datetime.datetime, datetime.timedelta, GenericNamedParameterPasser)):
        return format_value('<<{}>>'.format(repr(value)))

    if not isinstance(value, six.string_types):
        raise Exception('Cannot format value `{}`: no handler for type {}'.format(
            str(value),
            type(value)))

    components = split_verbatim(value)

    if len(components) == 1:
        return components[0]

    return '({})'.format(' + '.join(components))


def comment(value):
    lines = value.split('\n')
    commented = ['# ' + line for line in lines]
    return '\n'.join(commented)
