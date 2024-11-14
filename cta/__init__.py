import ssl
from abc import ABC, abstractmethod
from ssl import SSLContext
from typing import Protocol

from jsonschema import validate
from pandas import DataFrame
from requests import Response, Session
from requests.adapters import HTTPAdapter


class API(ABC):
    """
    An abstract base class (ABC) to be inherited by all downstream API wrappers.
    """  # noqa: E501

    @abstractmethod
    def get(self) -> DataFrame:
        """
        Get and validate the data at a REST API endpoint

        :return: A pandas.DataFrame of the content of the REST API data
        :rtype: DataFrame
        """
        ...


class API_PROTOCOL(Protocol):
    """
    A Protocol class to define an API class's global variables

    The current set of global variables are:

    * queryTime: float    -> The UNIX timestamp of when a query was made
    * endpointBase: str   -> The base url of a CTA REST API endpoint
    """

    queryTime: float
    endpointBase: str


class SSLAdapter(HTTPAdapter):
    """
    The purpose of this code is to override the init_poolmanager method, which is responsible for initializing a pool of connections to a server. In this case, it's used to pass the ssl_context instance to the underlying connection pool manager.

    By doing so, when creating a connection pool using this adapter, the connections will be established with SSL/TLS encryption enabled, using the provided context.
    """  # noqa: E501

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)


def get(url: str) -> Response:
    """
    A generic HTTP GET request that leverages TLSv1 connections.

    :param url: A URL to submit the HTTP GET request to
    :type url: str
    :return: A requests.Response object
    :rtype: Response
    """
    context: SSLContext = ssl.create_default_context()
    context.set_ciphers("DEFAULT:@SECLEVEL=1")

    session: Session = Session()
    session.mount("https://", SSLAdapter(ssl_context=context))

    return session.get(url=url, timeout=60)


def validateData(data: dict, schema: dict) -> bool:
    """
    Given JSON data from a requests.Response().json() and a JSON schema object, validate that the JSON data matches the JSON schema

    :param data: JSON data
    :type data: dict
    :param schema: JSON Schema
    :type schema: dict
    :return: True if the JSON data matches the JSON schema, else jsonschema.ValidationError is raised
    :rtype: bool
    """  # noqa: E501
    validate(instance=data, schema=schema)
    return True
