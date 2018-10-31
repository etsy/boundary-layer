import marshmallow as ma
import pytest
from boundary_layer.oozier.jsp_macros import JspMacroTranslator


def test_macro_translator():
    """ Make sure we properly translate known macros contained in strings,
        lists, or dictionary values
    """
    translator = JspMacroTranslator({
        'key0': '{{macros.key0}}',
        'key1': '{{macros.key1}}',
        'wf.id()': '{{dag.dag_id}}',
    })

    assert translator.translate('${key0}') == '{{macros.key0}}'
    assert translator.translate(['${key0}', 'this is a test ${key1}']) == [
        '{{macros.key0}}',
        'this is a test {{macros.key1}}',
    ]
    assert translator.translate({
        'workflow_id': '${wf.id()}',
        'key': '${key0}!!',
    }) == {
        'workflow_id': '{{dag.dag_id}}',
        'key': '{{macros.key0}}!!',
    }


def test_macro_translator_unknown_macro():
    """ Should throw an exception on an unrecognized macro """
    translator = JspMacroTranslator({
        'key0': '{{macros.key0}}',
        'key1': '{{macros.key1}}',
        'wf.id()': '{{dag.dag_id}}',
    })

    with pytest.raises(ma.ValidationError):
        translator.translate('${newKey}')


def test_macro_translator_unknown_input_type():
    """ Should throw an exception when the input is not one of the supported
        data types
    """
    translator = JspMacroTranslator({
        'key0': '{{macros.key0}}',
        'key1': '{{macros.key1}}',
        'wf.id()': '{{dag.dag_id}}',
    })

    with pytest.raises(ma.ValidationError):
        translator.translate(set(['${newKey}']))
