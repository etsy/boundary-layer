import pytest
from boundary_layer.containers import ExecutionContext


def test_operator_registry(valid_operator_registry):
    operator_config = {
        'name': 'my-operator',
        'type': 'base',
    }

    operator = valid_operator_registry.get(operator_config)
    operator.resolve_properties(
        execution_context=ExecutionContext(None, {}),
        default_task_args={},
        base_operator_loader=valid_operator_registry.get,
        preprocessor_loader=None)

    assert operator.resolved_properties.values == {
        'dag': '<<dag>>',
        'task_id': 'my_operator'
    }


def test_reject_unknown_properties(valid_operator_registry):
    operator_config = {
        'name': 'my-operator',
        'type': 'base',
        'properties': {
            'not_a_known_property': True,
        }
    }

    operator = valid_operator_registry.get(operator_config)
    with pytest.raises(Exception):
        operator.resolve_properties(
            execution_context=ExecutionContext(None, {}),
            default_task_args={},
            base_operator_loader=valid_operator_registry.get,
            preprocessor_loader=None)


def test_accept_unknown_properties_for_flexible_schema(valid_operator_registry):
    operator_config = {
        'name': 'my-operator',
        'type': 'allow_extra_properties',
        'properties': {
            'not_a_known_property': True,
        }
    }

    operator = valid_operator_registry.get(operator_config)
    operator.resolve_properties(
        execution_context=ExecutionContext(None, {}),
        default_task_args={},
        base_operator_loader=valid_operator_registry.get,
        preprocessor_loader=None)

    assert operator.resolved_properties.values == {
        'dag': '<<dag>>',
        'task_id': 'my_operator',
        'not_a_known_property': True,
    }
