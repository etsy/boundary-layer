import pytest
from boundary_layer.plugins import BasePlugin, PluginManager


def test_plugin_manager_default():
    """  Basic test to confirm the configuration loaded from the default plugin
         for the java operator type, used to make sure that the next test is set
         up correctly (ie this test makes sure that the appropriate value is
         set when the test plugin is not included)
    """
    mgr = PluginManager()

    assert mgr.operators({
        'type': 'dataproc_hadoop',
        'name': 'my-operator',
        'properties': {'main_class': 'org.testing.MyTestClass'},
    }).operator_class == 'DataProcHadoopOperator'


class PluginForTesting(BasePlugin):
    name = 'test-plugin'

    configs_path = 'test/plugins/test_plugin_config'


def test_plugin_manager_multiple_priorities():
    """ Make sure that the custom plugin's `java` operator overrides the default
        plugin's version of that operator
    """
    mgr = PluginManager(
        plugins=[PluginForTesting()],
        load_package_plugins=False)

    assert mgr.operators({
        'type': 'java',
        'name': 'my-operator',
        'properties': {'main_class': 'org.testing.MyTestClass'},
    }).operator_class == 'MyCustomClass'


def test_plugin_manager_duplicate_priorities():
    """ Make sure that the plugin manager raises an exception when multiple
        different plugins define the same operator
    """

    # Note, the plugin manager does not currently compare to make sure that the
    # two definitions are different before raising an error, so we can just
    # pass two instances of the same plugin in order to induce the error
    mgr = PluginManager(
        plugins=[PluginForTesting(), PluginForTesting()],
        load_package_plugins=False)

    with pytest.raises(Exception):
        print(mgr.operators({
            'type': 'java',
            'name': 'my-operator',
            'properties': {'main_class': 'org.testing.MyTestClass'},
        }).operator_class)
