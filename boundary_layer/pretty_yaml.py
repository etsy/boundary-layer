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

# Simple utilities for writing out YAML configs in a more readable format: in particular,
# with singleton values appearing before arrays, shorter arrays before longer arrays,
# and certain higher-priority keys appearing before everything else.
# Based on an OrderedDict-representer approach described here:
# https://stackoverflow.com/questions/5121931/in-python-how-can-you-load-yaml-mappings-as-ordereddicts
import functools
from collections import OrderedDict
import yaml
from six.moves import map


def _build_dumper():
    class OrderedDumper(yaml.SafeDumper):
        pass

    def _dict_representer(dumper, data):
        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            data.items())

    OrderedDumper.add_representer(OrderedDict, _dict_representer)

    return OrderedDumper


PRIORITY_KEYS = [
    'name',
    'type',
    'compatibility_version',
    'plugin_config',
    'imports',
    'dag_args',
    'resources',
    'default_task_args',
    'before',
    'operators',
    'after',
    'sub_dags',
    'generators']


def _reorder(item):
    if not isinstance(item, dict):
        return item

    def scorer(key, value):
        if key in PRIORITY_KEYS:
            return PRIORITY_KEYS.index(key) - len(PRIORITY_KEYS)

        value_score = len(value) if isinstance(value, (dict, list)) else 0

        return value_score

    def comparator(kv0, kv1):
        score0 = scorer(*kv0)
        score1 = scorer(*kv1)

        if score0 != score1:
            return 1 if score0 > score1 else -1
        else:
            (key0, _) = kv0
            (key1, _) = kv1

            if key0 == key1:
                return 0

            return 1 if key0 > key1 else -1

    result = OrderedDict()
    for (key, value) in sorted(item.items(), key=functools.cmp_to_key(comparator)):
        if isinstance(value, dict):
            result[key] = _reorder(value)
            continue

        if isinstance(value, list):
            result[key] = list(map(_reorder, value))
            continue

        result[key] = value

    return result


def dump_all(items):
    return yaml.dump_all(
        list(map(_reorder, items)),
        None,
        _build_dumper(),
        default_flow_style=False)


def dump(item):
    return dump_all([item])
