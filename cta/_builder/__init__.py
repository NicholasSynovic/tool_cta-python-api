import typing
import urllib.parse


def _constructAPI(url: str, **kwargs) -> str:
    """
    Given a URL and parameters (represented as key word arguements), create a new URL with the encoded parameters.

    Only parameters with a non-null value are appended to the URL.

    :param url: A URL represented as a string
    :type url: str
    :return: A formatted URL with non-null parameters
    :rtype: str
    """  # noqa: E501
    data: dict[str, typing.Any] = {}

    key: str
    val: typing.Any
    for key, val in kwargs.items():
        if val is None:
            continue

        if len(str(val)) == 0:
            continue

        data[key] = val

    params: str = urllib.parse.urlencode(query=data)
    return f"{url}?{params}"


def _safeJoin(data: typing.Any, sep: str = ",") -> str | None:
    """
    Given some data, try to create a string of the data where each value is seperated by a specific char or string

    String data is not modified. List data is joined by the seperator. All other datatypes are returned as None.

    :param data: Data to join together
    :type data: typing.Any
    :param sep: Char or string to seperate data values by, defaults to ","
    :type sep: str, optional
    :return: A string with each value seperated or None
    :rtype: str | None
    """  # noqa: E501
    if isinstance(data, str):
        return data

    if isinstance(data, list):
        return f"{sep}".join(data)

    return None
