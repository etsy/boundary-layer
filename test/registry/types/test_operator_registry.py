from boundary_layer.containers import ExecutionContext

def test_operator_registry(valid_operator_registry):
    operator_config = {
        'name': 'my-operator',
        'type': 'base',
    }

    operator = valid_operator_registry.get(operator_config)
    print(operator)
    operator.resolve_properties(
            execution_context=ExecutionContext(None, {}),
            default_task_args={},
            base_operator_loader=valid_operator_registry,
            preprocessor_loader=None)

    assert operator.resolved_properties.values == {
        'dag': '<<dag>>',
        'task_id': 'my_operator'
    }
