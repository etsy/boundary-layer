import functools
import pytest
from boundary_layer import util


@pytest.fixture
def fc_node_builder():
    return functools.partial(util.make_flow_control_node, default_task_args={})
