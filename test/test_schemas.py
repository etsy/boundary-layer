from boundary_layer.schemas.dag import BatchingSchema


_batching_schema = BatchingSchema()


def _load_and_validate(data):
    loaded = _batching_schema.load(data)
    assert not loaded[1]

    return loaded[0]


def _dump_and_validate(data):
    dumped = _batching_schema.dump(data)
    assert not dumped[1]

    return dumped[0]


def test_batching_schema_implicit_enabled():
    data = {
        'batch_size': 10
    }
    batching = _load_and_validate(data)

    assert 'disabled' not in batching
    assert batching['batch_size'] == 10

    dumped = _dump_and_validate(batching)

    assert 'disabled' not in dumped
    assert dumped['batch_size'] == 10


def test_batching_schema_explicit_enabled():
    data = {
        'disabled': False,
        'batch_size': 10
    }
    batching = _load_and_validate(data)

    assert batching['disabled'] is False
    assert batching['batch_size'] == 10

    dumped = _dump_and_validate(batching)

    assert dumped['disabled'] is False
    assert dumped['batch_size'] == 10


def test_batching_schema_disabled():
    data = {
        'disabled': True,
        'batch_size': 10
    }
    batching = _load_and_validate(data)

    assert batching['disabled'] is True
    assert batching['batch_size'] == 10

    dumped = _dump_and_validate(batching)

    assert dumped['disabled'] is True
    assert dumped['batch_size'] == 10
