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
import operator
import hashlib
from six.moves import map
import six
from boundary_layer.containers import ExecutionContext


class GenericNamedParameterPasser(object):

    def __init__(self, typ, args):
        self.typ = typ
        self.args = args

    def __repr__(self):
        return '{}({})'.format(self.typ,
                               ",".join([k + "=" + repr(v) for k, v in self.args.items()]))


def freeze(item):
    if isinstance(item, dict):
        return frozenset((key, freeze(value)) for (key, value) in six.iteritems(item))

    if isinstance(item, (list, tuple)):
        return tuple(map(freeze, item))

    if isinstance(item, set):
        return frozenset(map(freeze, item))

    return item


def md5(item):
    digest = hashlib.md5()

    digest.update(item.encode('utf-8'))

    return digest.hexdigest()


def make_flow_control_node(deps, name=None, default_task_args=None):
    """ Make a flow control node.
    """
    from boundary_layer import plugins

    sorted_deps = sorted(deps, key=operator.attrgetter('name'))

    def build_hash():
        return md5('-'.join(dep.name for dep in sorted_deps))

    def name_from_deps():
        max_name_len = 60
        hash_len = 6
        trailing_len = len('-fc')
        chars_per_dep = -1 + (max_name_len - hash_len -
                              trailing_len) // len(deps)

        return '-'.join([dep.name[:chars_per_dep] for dep in sorted_deps] +
                        [build_hash()[:hash_len], 'fc'])

    fc_node = plugins.manager.operators({
        'name': name or name_from_deps(),
        'type': 'flow_control',
        'upstream_dependencies': list(deps),
    })
    fc_node.resolve_properties(
        default_task_args=default_task_args,
        execution_context=ExecutionContext(referrer=None, resources={}),
        base_operator_loader=plugins.manager.operators,
        preprocessor_loader=plugins.manager.property_preprocessors)

    return fc_node


def merge_schemas(parent, child):
    """ Merge two json schemas.  Note that this method is asymmetric;
        if the child redefines a field present in the parent, the child's
        definition takes precedence.  But if either of them provide
        fields in the `requires` directive, those will be merged.

        :param parent: the parent operator's json schema
        :type parent: dict
        :param child: the child operator's json schema
        :type child: dict
        :returns: a new jsonschema reflecting the merger
        :rtype: dict
    """
    result = {'properties': {}}
    result['properties'].update(parent.get('properties', {}))
    result['properties'].update(child.get('properties', {}))

    required = parent.get('required', []) + child.get('required', [])
    if required:
        result['required'] = required

    for schema in [parent, child]:
        if 'additionalProperties' in schema:
            result['additionalProperties'] = schema['additionalProperties']

    extra_keys = (frozenset(parent) | frozenset(child)) - \
        frozenset(['properties', 'required', 'additionalProperties'])

    if extra_keys:
        raise Exception(
            'Cannot merge schemas due to unrecognized fields: {} '
            '(parent schema: {}; child schema: {})'.format(
                extra_keys,
                parent,
                child))

    return result


def sanitize_operator_name(name):
    # make operator names snake_case
    if isinstance(name, list):
        return list(map(sanitize_operator_name, name))
    elif isinstance(name, six.string_types):
        assert name[0].isalpha() or name[0] == '_', \
            'Invalid name `{}`: must start with a character!'.format(name)

        return re.sub('[^a-zA-Z0-9_]', '_', name)

    raise Exception('Cannot sanitize name `{}`'.format(name))
