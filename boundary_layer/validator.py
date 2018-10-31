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

from copy import deepcopy
import six
from jsonschema import Draft4Validator, validators

# Copy the extend_with_defaults method from the JSON Schema docs:
# http://python-jsonschema.readthedocs.io/en/latest/faq/


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for _property, subschema in six.iteritems(properties):
            if "default" in subschema:
                instance.setdefault(_property, subschema["default"])

        for error in validate_properties(
                validator, properties, instance, schema):
            yield error

    return validators.extend(
        validator_class, {"properties": set_defaults},
    )


DefaultValidatingDraft4Validator = extend_with_default(Draft4Validator)


def validate_and_fill_defaults(item, schema):
    copy = deepcopy(item)

    DefaultValidatingDraft4Validator(schema).validate(copy)

    return copy
