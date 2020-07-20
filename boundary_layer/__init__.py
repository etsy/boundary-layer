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

import pkg_resources
import semver


def get_version_string():
    version = pkg_resources.get_distribution('boundary_layer').version

    # Pip seems to replace '-' with '.' in the version strings, for some reason.
    # this makes semver unhappy, so we must replace .dev0 with -dev0
    if not version.endswith('.dev0'):
        return version

    dev0_position = version.rindex('.dev0')
    return version[:dev0_position] + '-' + version[1 + dev0_position:]


VERSION_STRING = get_version_string()
VERSION = semver.parse_version_info(VERSION_STRING)
MIN_SUPPORTED_VERSION = semver.parse_version_info('0.9.9')

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
