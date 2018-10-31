from boundary_layer.registry import NodeTypes
from boundary_layer.containers import ExecutionContext
from boundary_layer import plugins


def test_default_param_filler():
    generator_config = {
        'name': 'do_snapshot_copiers',
        'type': 'requests_json_generator',
        'target': 'do_something_with_items',
        'requires_resources': ['dataproc-cluster'],
        'properties': {
            'url': 'http://my.host.com/my-endpoint',
            'list_json_key': 'my-key',
        }
    }

    g = plugins.manager.generators(generator_config)

    assert g.type == NodeTypes.GENERATOR

    assert g.resolve_properties(ExecutionContext(referrer=None, resources={})).values == {
        'url': 'http://my.host.com/my-endpoint',
        'list_json_key': 'my-key',
        'timeout_sec': 5,
        'headers': {}
    }
