import functools
from argparse import Namespace
import pytest
from boundary_layer import util
from boundary_layer.registry.types import OperatorRegistry


@pytest.fixture
def fc_node_builder():
    return functools.partial(util.make_flow_control_node, default_task_args={})


@pytest.fixture
def parse_oozie_default_args():
    return Namespace(
        cluster_base_name=None,
        cluster_depends_on_past=False,
        cluster_name_suffix=None,
        cluster_num_workers=128,
        cluster_project_id='my-project',
        cluster_region='us-central1',
        cluster_wait_for_downstream=False,
        dag_concurrency=16,
        dag_disable_catchup=False,
        dag_max_active_runs=1,
        dag_name=None,
        dag_schedule_interval='@daily',
        only_nodes=None,
        prune_nodes=None,
        with_external_task_sensors=[],
        workflow_name='test-workflow',
        )


@pytest.fixture
def valid_operator_registry():
    path = "test/data/registry/valid/operators"
    return OperatorRegistry([path])
