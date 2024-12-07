from typing import List

import pytest

from cta._builder import _constructAPI, _safeJoin


def test_constructAPI() -> None:
    url: str = "https://test.com"

    assert _constructAPI(url=url, test="test") == "https://test.com?test=test"
    assert _constructAPI(url=url, test=None) == "https://test.com?"
    assert _constructAPI(url=url, test="") == "https://test.com?"
    assert (
        _constructAPI(url=url, test1=1, test2=2) == "https://test.com?test1=1&test2=2"
    )


def test_safeJoint() -> None:
    string: str = "hello world"
    stringList: List[str] = ["hello", "world"]
    intList: List[int] = [1, 2]

    assert _safeJoin(data=string) == string
    assert _safeJoin(data=stringList) == "hello,world"

    with pytest.raises(expected_exception=TypeError):
        _safeJoin(data=intList)
