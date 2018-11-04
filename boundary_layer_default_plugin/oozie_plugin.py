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

from collections import namedtuple
from boundary_layer.oozier.cluster_config import DataprocHadoopClusterConfig
from boundary_layer.plugins import BaseOozieParserPlugin
from .oozie_actions import \
        OozieSubWorkflowBuilder, \
        OozieFileSystemActionBuilder, \
        OozieSshActionBuilder, \
        OozieMapReduceActionBuilder


ExternalTaskSpec = namedtuple('ExternalTaskSpec', ['dag_id', 'task_id'])


def _external_task_spec(item):
    """ A parser for the --with-external-task-sensors argument """
    if item.count(':') != 1:
        raise ValueError(
            'Invalid external task specification `{}`: expected a '
            'string of the form <dag_id>:<task_id>'.format(item))

    return ExternalTaskSpec(*item.split(':'))


class DefaultOozieParserPlugin(BaseOozieParserPlugin):
    @classmethod
    def register_arguments(cls, parser):
        parser.add_argument('--with-external-task-sensors',
                            type=_external_task_spec,
                            nargs='+',
                            default=[])

        parser.add_argument(
            '--dag-max-active-runs',
            type=int,
            help='Argument for DAG max_active_runs parameter')
        parser.add_argument(
            '--dag-schedule-interval',
            help='Argument for DAG schedule_interval parameter')
        parser.add_argument(
            '--dag-disable-catchup',
            default=False,
            action='store_true',
            help='Whether to set the DAG parameter catchup=False')
        parser.add_argument(
            '--dag-concurrency',
            type=int,
            help='argument for DAG concurrency parameter')

    def action_builders(self):
        return [
            OozieSubWorkflowBuilder,
            OozieFileSystemActionBuilder,
            OozieSshActionBuilder,
            OozieMapReduceActionBuilder,
        ]

    def upstream_operators(self):
        result = []
        for sensor in self.args.with_external_task_sensors:
            result.append({
                'name': '{}-{}-sensor'.format(sensor.dag_id, sensor.task_id),
                'type': 'external_task_sensor',
                'properties': {
                    'external_dag_id': sensor.dag_id,
                    'external_task_id': sensor.task_id,
                }
            })

        return result

    def jsp_macros(self):
        return {
            'wf:id()': '{{ task.task_id }}',
            'wf:name()': '{{ task.task_id }}',
        }

    def dag_args(self):
        result = {}

        if self.args.dag_max_active_runs:
            result['max_active_runs'] = self.args.dag_max_active_runs

        if self.args.dag_schedule_interval:
            result['schedule_interval'] = self.args.dag_schedule_interval

        result['catchup'] = not self.args.dag_disable_catchup

        if self.args.dag_concurrency:
            result['concurrency'] = self.args.dag_concurrency

        return result

    def cluster_config(self):
        return DataprocHadoopClusterConfig(
            project_id='my-project',
            region='us-central1',
            cluster_name='my-cluster',
            num_workers=128)
