import pytest
from boundary_layer_default_plugin.preprocessors import *


def test_ensure_rendered_string_pattern():
    pattern = "[a-z][a-z0-9-]{,54}(?<!-)"  # this is the dataproc cluster-create pattern
    renderer = EnsureRenderedStringPattern({'pattern': pattern})

    assert renderer.process_arg("my-cluster") == "my-cluster"
    assert renderer.process_arg("my-cluster-{{ds}}") == "my-cluster-{{ds}}"
    assert renderer.process_arg(
        "my-cluster-{{execution_date.strftime('%Y-%m-%d')}}"
    ) == "my-cluster-{{execution_date.strftime('%Y-%m-%d')}}"

    with pytest.raises(Exception):
        renderer.process_arg("my-cluster-")

    with pytest.raises(Exception):
        renderer.process_arg("my-cluster-{{ds}}-")

    with pytest.raises(Exception):
        renderer.process_arg("My-cluster")

    with pytest.raises(Exception):
        renderer.process_arg("my-cluster-{{execution_date.strftime('%Y_%m_%d')}}")
