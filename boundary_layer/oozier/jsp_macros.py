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
import six
from six.moves import map
import marshmallow as ma


class JspMacroTranslator(object):
    # Oozie macros use JSP Expression Language.  I can't find a convenient
    # expression evaluator, but there aren't too many things we need to support
    # so we can do it with regexes and substitutions
    OOZIE_MACRO_RE = re.compile(r'\$\{[^}]+\}')

    def __init__(self, macros):
        self.macros = macros

    def translate(self, item):
        if isinstance(item, dict):
            return {
                key: self.translate(value) for (key, value) in six.iteritems(item)
            }
        elif isinstance(item, list):
            return list(map(self.translate, item))

        if not isinstance(item, six.string_types):
            raise ma.ValidationError('Cannot translate object {} of type {}'.format(
                item,
                type(item)))

        hits = list(self.OOZIE_MACRO_RE.finditer(item))
        if not hits:
            return item

        split = self.OOZIE_MACRO_RE.split(item)
        # Some sanity checking to make sure that the split gave us what we expected.
        # Given that we know there's at least one hit, we expect that there should be
        # at least one more item in splits than in hits.  If the text field both
        # begins and ends with a hit, then there can potentially be 2 more items
        # in the split array.
        if not (2 + len(hits) >= len(split) > len(hits)):
            raise ma.ValidationError(
                'Bad split on input text `{}`: hits == `{}` / split == `{}`'.format(
                    item,
                    hits,
                    split))

        if not(split[-1] == '' or len(split) == 1 + len(hits)):
            raise ma.ValidationError(
                'Bad split on input text `{}`: hits == `{}` / split == `{}`'.format(
                    item,
                    hits,
                    split))

        result = split[0]
        for (idx, hit) in enumerate(hits):
            # Must discard the first 2 and final 1 characters from the hit, because
            # these represent the '${' and '}'.  Note that we could use capture groups
            # in the regex but then these will contaminate the output of the split()
            # function (see: https://docs.python.org/2/library/re.html#re.split)
            assert hit.group(0).startswith('${') and hit.group(0).endswith('}')
            oozie_macro = hit.group(0)[2:-1]
            translation = self.macros.get(oozie_macro)

            if translation is None:
                raise ma.ValidationError(
                    'Could not translate {}: unrecognized macro `{}`'.format(item, oozie_macro))

            # if we've made it here, either translation was successful or
            # unsuccessful translations are permitted.  In the latter case,
            # we reinsert the oozie macro.
            result += translation or hit.group(0)
            result += split[1 + idx]

        return result
