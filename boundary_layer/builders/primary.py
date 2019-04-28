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
import boundary_layer
from boundary_layer.builders.base import DagBuilderBase
from boundary_layer.schemas.dag import DagArgsSchema


class PrimaryDagBuilder(DagBuilderBase):
    indent_operators = False

    def preamble(self):
        assert len(self.reference_path) == 1, \
            'Invalid Primary DAG reference path: {}'.format(
                self.reference_path)

        template = self.get_jinja_template('primary_preamble.j2')

        dag_args_dumped = DagArgsSchema(context={'for_dag_output': True}).dump(
            self.dag.get('dag_args', {}))
        if dag_args_dumped.errors:
            # should not happen because the schema was validated upon load,
            # but we should check
            raise Exception('Error serializing dag_args: {}'.format(
                dag_args_dumped.errors))

        dag_args = dag_args_dumped.data
        dag_args['dag_id'] = self.build_dag_id()

        default_task_args = self.dag.get('default_task_args', {})

        return template.render(
            dag_args=dag_args,
            default_task_args=default_task_args,
            imports=self.get_imports(),
            specs=self.specs,
            metadata=self.metadata,
            config_md5=boundary_layer.util.md5(self.metadata.yaml_config),
            build_time=datetime.datetime.utcnow().isoformat(),
            library_name='boundary-layer',
            library_version=boundary_layer.get_version_string(),
        )

    def epilogue(self):
        pass
