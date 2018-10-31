import pytest

from boundary_layer.builders import util


def test_sanitize_operator_name():
    assert util.sanitize_operator_name('test-operator') == 'test_operator'
    assert util.sanitize_operator_name('test_operator') == 'test_operator'

    assert util.sanitize_operator_name(['test-operator-1', 'test.operator.2']) == \
        ['test_operator_1', 'test_operator_2']

    with pytest.raises(Exception):
        util.sanitize_operator_name('-test')


def test_verbatim():
    assert util.verbatim('test-item') == '<<test-item>>'
    assert util.verbatim(['test-item-1', 'test-item-2']) == [
        '<<test-item-1>>', '<<test-item-2>>']

    with pytest.raises(Exception):
        util.verbatim(10)


def test_construct_dag_name():
    assert util.construct_dag_name(['my', 'dag-', 'path!']) == 'my.dag_.path_'


def test_subdag_builder_name():
    assert util.subdag_builder_name('my-swf') == 'my_swf_builder'


def test_add_leading_spaces():
    assert util.add_leading_spaces("""
this is a test
this is another test""", 1) == "    \n    this is a test\n    this is another test"

    assert util.add_leading_spaces("""
this is a test
this is another test""", 2) == "        \n        this is a test\n        this is another test"

    assert util.add_leading_spaces("\n\nthis is another test", 1) == \
        "    \n    \n    this is another test"


def test_enquote():
    assert util.enquote('hi there') == "'hi there'"
    assert util.enquote("hi 'there'") == """"hi 'there'" """.strip()

    assert util.enquote(
        'hi "there" \'friend\'') == '"""hi \"there\" \'friend\'"""'

    with pytest.raises(Exception):
        assert util.enquote('hi """there""" \'friend\'')


def test_split_verbatim():
    assert util.split_verbatim('<<10 + 11>>=21') == ['(10 + 11)', "'=21'"]
    assert util.split_verbatim('<<10 + 11>>') == ['(10 + 11)']
    assert util.split_verbatim(
        '<<10 + 11>><<12+13>>') == ['(10 + 11)', '(12+13)']
    assert util.split_verbatim(
        '<<10 + 11>> <<12+13>>') == ['(10 + 11)', "' '", '(12+13)']

    with pytest.raises(Exception):
        util.split_verbatim('<<10 + 11>')


def test_format_value():
    assert util.format_value('cats') == "'cats'"
    assert util.format_value('cats <<dogs>>') == "('cats ' + (dogs))"
    assert util.format_value(['cats <<dogs>>', 'dogs <<cats>>']) == \
        "[('cats ' + (dogs)),('dogs ' + (cats))]"

    assert util.format_value(10) == '10'

    assert util.format_value({'dogs': '<<cats>>'}) == "{ 'dogs': (cats) }"

    assert util.format_value(None) == 'None'

    with pytest.raises(Exception):
        assert util.format_value(set(10))


def test_comment():
    assert util.comment('hello') == '# hello'
    assert util.comment('hello\nthere') == '# hello\n# there'
