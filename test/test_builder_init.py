from typing import List

import pytest

from cta._builder import (
    _checkInList,
    _checkNotNoneAndType,
    _constructAPI,
    _safeJoin,
    _checkArgIsValidListOrString,
)


def test_constructAPI() -> None:
    url: str = "https://test.com"

    assert _constructAPI(url=url, test="test") == "https://test.com?test=test"
    assert _constructAPI(url=url, test=None) == f"{url}?"
    assert _constructAPI(url=url, test="") == f"{url}?"
    assert _constructAPI(url=url, test1=1, test2=2) == f"{url}?test1=1&test2=2"


def test_safeJoin() -> None:
    string: str = "hello world"
    stringList: List[str] = ["hello", "world"]
    intList: List[int] = [1, 2]

    assert _safeJoin(data=string) == string
    assert _safeJoin(data=stringList) == "hello,world"

    with pytest.raises(expected_exception=TypeError):
        _safeJoin(data=intList)


def test_checkNotNoneAndType() -> None:
    assert _checkNotNoneAndType(obj="Test", types=str) == True
    assert _checkNotNoneAndType(obj="Test", types=(str, int)) == True

    assert _checkNotNoneAndType(obj=None, types=str) == False
    assert _checkNotNoneAndType(obj=1, types=str) == False
    assert _checkNotNoneAndType(obj=1, types=(str, list)) == False

    with pytest.raises(expected_exception=TypeError):
        _checkNotNoneAndType(obj=1, types=[str, list])
        _checkNotNoneAndType(obj=1, types=1)
        _checkNotNoneAndType(obj=1, types="")


def test_checkInList() -> None:
    assert _checkInList(obj="test", _list=["test"]) is None
    assert _checkInList(obj="test", _list=["test", "tree"]) is None

    with pytest.raises(expected_exception=AttributeError):
        assert _checkInList(obj="test", _list=1)
        assert _checkInList(obj="test", _list="test")

    with pytest.raises(ValueError):
        _checkInList(obj=1, _list=["test"])


def test_checkArgIsValidListOrString()  ->  None:
    assert _checkArgIsValidListOrString(arg="test", parameterName="test", validObj=["test"],) is None

    assert _checkArgIsValidListOrString(arg=["test"], parameterName="test", validObj=["test"],) is None

    with pytest.raises(TypeError):
        _checkArgIsValidListOrString(arg=1, parameterName="test", validObj=["test"],)

    with pytest.raises(expected_exception=ValueError):
        _checkArgIsValidListOrString(arg="hello", parameterName="test", validObj=["world"],)
        _checkArgIsValidListOrString(arg=["hello"], parameterName="test", validObj=["world"],)
