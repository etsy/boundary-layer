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

import sys
import logging
from boundary_layer import VERSION_STRING


def setup_logger():
    result = logging.getLogger('boundary-layer v. {}'.format(VERSION_STRING))

    stream = logging.StreamHandler(sys.stderr)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream.setFormatter(formatter)

    result.addHandler(stream)
    result.setLevel(logging.INFO)

    return result


logger = setup_logger()
