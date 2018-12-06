from boundary_layer.schemas.dag import BatchingSchema


def test_batching_schema_implicit_enabled():
    schema = BatchingSchema()
    data = {
        'batch_size': 10
    }
    batching = schema.load(data)[0]

    assert batching['enabled'] is True
    assert batching['batch_size'] == 10
    assert batching['original_enabled'] is None

    dumped = schema.dump(batching)[0]

    assert 'enabled' not in dumped
    assert dumped['batch_size'] == 10
    assert 'original_enabled' not in dumped


def test_batching_schema_explicit_enabled():
    schema = BatchingSchema()
    data = {
        'enabled': True,
        'batch_size': 10
    }
    batching = schema.load(data)[0]

    assert batching['enabled'] is True
    assert batching['batch_size'] == 10
    assert batching['original_enabled'] is True

    dumped = schema.dump(batching)[0]

    assert dumped['enabled'] is True
    assert dumped['batch_size'] == 10
    assert 'original_enabled' not in dumped


def test_batching_schema_disabled():
    schema = BatchingSchema()
    data = {
        'enabled': False,
        'batch_size': 10
    }
    batching = schema.load(data)[0]

    assert batching['enabled'] is False
    assert batching['batch_size'] == 10
    assert batching['original_enabled'] is False

    dumped = schema.dump(batching)[0]

    assert dumped['enabled'] is False
    assert dumped['batch_size'] == 10
    assert 'original_enabled' not in dumped
