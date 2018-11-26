# We must import the plugin manager in order to preload the plugin
from boundary_layer.plugins import manager
from boundary_layer_default_plugin.plugin import DefaultPlugin


def test_valid_plugin():
    """ Make sure we can instantiate the default plugin.  This will
        flag any errors that may exist in the plugin's registries.
    """
    plugin = DefaultPlugin()
    assert plugin is not None
