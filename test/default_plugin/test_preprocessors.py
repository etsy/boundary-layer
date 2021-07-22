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

    debug_mock = Mock()
    mocker.patch.object(logger, 'debug', new=debug_mock)
    assert renderer.process_arg(
        "my-cluster-{{task.task_id}}",
        None,
        {'task_id': 'my-task-id'},
    ) == "my-cluster-{{task.task_id}}"
    debug_mock.assert_not_called()

    assert renderer.process_arg(
        "my-cluster-{{unknown_var.task_id}}",
        None,
        {'task_id': 'my-task-id'},
    ) == "my-cluster-{{unknown_var.task_id}}"
    debug_mock.assert_called_once()

    debug_mock = Mock()
    mocker.patch.object(logger, 'debug', new=debug_mock)
    assert renderer.process_arg(
        "my-<<item_name>>-cluster", None, {},
    ) == "my-<<item_name>>-cluster"
    debug_mock.assert_called_once()

    debug_mock = Mock()
    mocker.patch.object(logger, 'debug', new=debug_mock)
    assert renderer.process_arg(
        "my-cluster-<<item.value>>", None, {},
    ) == "my-cluster-<<item.value>>"
    debug_mock.assert_called_once()

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

    with pytest.raises(Exception):
        renderer.process_arg("My-<item.value>-cluster", None, {})


def test_pubsub_message_to_binary_string_dictionary(mocker):
    """
    Given: User provides message with `dict` type data property
    Assert: Data value is converted to json and encoded into binary string
    """
    preprocessed_obj = {'hello': 'world'}
    preprocessed_messages = [
        {
            'data': preprocessed_obj,
            'attributes': {'testing': 'is_cool'}
        }
    ]
    expected_obj_str_encoded = \
        base64.b64encode(json.dumps(preprocessed_obj).encode('utf-8')).decode('utf-8')
    processor = PubsubMessageDataToBinaryString({})
    res = processor.process_arg(preprocessed_messages, None, {})
    assert res[0]['data'] == expected_obj_str_encoded


def test_pubsub_message_to_binary_string_str(mocker):
    """
    Given: User provides message with `str` type data property
    Assert: Data value is encoded into binary string
    """
    preprocessed_str = 'helloworld'
    preprocessed_messages = [
        {
            'data': preprocessed_str
        }
    ]
    expected_str_encoded = \
        base64.b64encode(preprocessed_str.encode('utf-8')).decode('utf-8')
    processor = PubsubMessageDataToBinaryString({})
    res = processor.process_arg(preprocessed_messages, None, {})
    assert res[0]['data'] == expected_str_encoded


def test_stringify_env_args():
    preprocessed_messages = {
        'ENV_VAR_STRING': 'some_string',
        'ENV_VAR_BOOLEAN1': True,
        'ENV_VAR_BOOLEAN2': False,
        'ENV_VAR_INT': 23,
        'ENV_VAR_DICT': {'testing': 'is_cool'},
        'ENV_VAR_LIST': ['abc', 'def'],
        'ENV_VAR_LIST_OF_DICTS': [{'a': 'b'}],
    }
    expected = {
        'ENV_VAR_STRING': 'some_string',
        'ENV_VAR_BOOLEAN1': 'true',
        'ENV_VAR_BOOLEAN2': 'false',
        'ENV_VAR_INT': '23',
        'ENV_VAR_DICT': '{"testing": "is_cool"}',
        'ENV_VAR_LIST': '["abc", "def"]',
        'ENV_VAR_LIST_OF_DICTS': '[{"a": "b"}]',
    }
    processor = StringifyObject({})
    res = processor.process_arg(preprocessed_messages, None, {})

    assert res == expected
