import ast
from textwrap import dedent

import pytest

from scripts.check_layout import missing_separators, separated


@pytest.mark.parametrize(
    "source",
    [
        "value = 1\nif value:\n    print(value)\nprint('done')\n",
        "def result():\n    value = 1\n    return value\n",
        "for value in []:\n    print(value)\nwhile False:\n    pass\n",
        "value = 1\n@decorator\ndef result():\n    return value\n",
        "try:\n    work()\nexcept ValueError:\n    recover()\nfinish()\n",
    ],
)
def test_separator_fixes_preserve_syntax_and_are_idempotent(source):
    missing = missing_separators(source)
    assert missing
    fixed = separated(source, missing)
    assert ast.dump(ast.parse(fixed)) == ast.dump(ast.parse(source))
    assert not missing_separators(fixed)
    assert separated(fixed, missing_separators(fixed)) == fixed


def test_literals_and_compound_branches_are_not_split():
    source = dedent('''\
        def result(value):
            if value:
                return """first
        second
        third"""
            elif value is None:
                return "missing"
            else:
                return "empty"
    ''')
    assert not missing_separators(source)


def test_guard_clause_and_related_assignments_stay_compact():
    source = "def result():\n    first = 1\n    second = 2\n\n    return first + second\n"
    assert not missing_separators(source)
