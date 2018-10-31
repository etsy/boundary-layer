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

import marshmallow as ma

# Define a base schema type that rejects any inputs with unrecognized arguments,
# to prevent silent mis-configurations.  Based on the docs here:
# http://marshmallow.readthedocs.io/en/latest/extending.html


class StrictSchema(ma.Schema):
    @ma.validates_schema(pass_original=True)
    def check_no_unknowns(self, _, original_data):
        def check_single_datum(datum):
            unknown = set(datum) - set(self.fields)
            if unknown:
                raise ma.ValidationError('Unknown fields present', unknown)

        if self.many:
            for item in original_data:
                check_single_datum(item)
        else:
            check_single_datum(original_data)
