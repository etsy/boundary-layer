import pytest
from boundary_layer.logger import logger
from boundary_layer_default_plugin.preprocessors import *
from mock import Mock


def test_ensure_rendered_string_pattern(mocker):
    pattern = "[a-z][a-z0-9-]{,54}(?<!-)"  # this is the dataproc cluster-create pattern
    renderer = EnsureRenderedStringPattern({'pattern': pattern})

    assert renderer.process_arg("my-cluster", None, {}) == "my-cluster"
    assert renderer.process_arg("my-cluster-{{ds}}", None, {}) == "my-cluster-{{ds}}"
    assert renderer.process_arg(
        "my-cluster-{{execution_date.strftime('%Y-%m-%d')}}",
        None,
        {},
    ) == "my-cluster-{{execution_date.strftime('%Y-%m-%d')}}"

    warning_mock = Mock()
    mocker.patch.object(logger, 'warning', new=warning_mock)
    assert renderer.process_arg(
        "my-cluster-{{task.task_id}}",
        None,
        {'task_id': 'my-task-id'},
    ) == "my-cluster-{{task.task_id}}"
    warning_mock.assert_not_called()

    assert renderer.process_arg(
        "my-cluster-{{unknown_var.task_id}}",
        None,
        {'task_id': 'my-task-id'},
    ) == "my-cluster-{{unknown_var.task_id}}"
    warning_mock.assert_called_once()

    with pytest.raises(Exception):
        renderer.process_arg("my-cluster-", None, {})

    with pytest.raises(Exception):
        renderer.process_arg("my-cluster-{{ds}}-", None, {})

    with pytest.raises(Exception):
        renderer.process_arg("My-cluster", None, {})

    with pytest.raises(Exception):
        renderer.process_arg(
            "my-cluster-{{execution_date.strftime('%Y_%m_%d')}}",
            None,
            {})
