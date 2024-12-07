import logging
from typing import Any, List, Tuple
from urllib.parse import urlencode


def _constructAPI(url: str, **kwargs) -> str:
    """
    Given a URL and parameters (represented as key word arguements), create a new URL with the encoded parameters.

    Only parameters with a non-null value are appended to the URL.

    :param url: A URL represented as a string
    :type url: str
    :return: A formatted URL with non-null parameters
    :rtype: str
    """  # noqa: E501
    data: dict[str, Any] = {}

    key: str
    val: Any
    for key, val in kwargs.items():
        if val is None:
            continue

        if len(str(val)) == 0:
            continue

        data[key] = val

    params: str = urlencode(query=data)
    return f"{url}?{params}"


def _safeJoin(data: Any, sep: str = ",") -> str | None:
    """
    Given some data, try to create a string of the data where each value is seperated by a specific char or string

    String data is not modified. List data is joined by the seperator. All other datatypes are returned as None.

    :param data: Data to join together
    :type data: Any
    :param sep: Char or string to seperate data values by, defaults to ","
    :type sep: str, optional
    :return: A string with each value seperated or None
    :rtype: str | None
    """  # noqa: E501
    if isinstance(data, str):
        return data

    if isinstance(data, list):
        try:
            return f"{sep}".join(data)
        except TypeError as e:
            logging.exception(msg=e)
            raise TypeError

    return None


def _checkNotNoneAndType(obj: Any, types: Any | Tuple[Any]) -> bool:
    """
    Return true if `obj` is not None and is of a valid type in `types`.

    :param obj: The object to test
    :type obj: Any
    :param types: The type or types (as a tuple) to test the object against
    :type types: Any | Tuple[Any]
    :raises TypeError: If `types` is not a type or a Tuple of types, raise TypeError
    :return: True if `obj` is not None and is of type `types`
    :rtype: bool
    """
    if obj is None:
        return False

    try:
        return isinstance(obj, types)
    except TypeError:
        raise TypeError("`types` must be a Tuple of types")


def _checkInList(obj: Any, _list: List[Any]) -> None:
    if _checkNotNoneAndType(obj=_list, types=(list, tuple)) is False:
        raise AttributeError("`_type` is not of type List or Tuple")

    try:
        _list.index(obj)
    except ValueError:
        raise ValueError(f"`{obj}` not in `_list`: `{_list}`")
