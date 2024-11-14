import ssl
from abc import ABC, abstractmethod
from ssl import SSLContext
from typing import Protocol

from jsonschema import ValidationError, validate
from pandas import DataFrame
from requests import Response, Session
from requests.adapters import HTTPAdapter


class API(ABC):
    @abstractmethod
    def get(self) -> DataFrame:
        ...


class API_PROTOCOL(Protocol):
    queryTime: float
    endpointBase: str


class SSLAdapter(HTTPAdapter):
    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)


def get(url: str) -> Response:
    context: SSLContext = ssl.create_default_context()
    context.set_ciphers("DEFAULT:@SECLEVEL=1")

    session: Session = Session()
    session.mount("https://", SSLAdapter(ssl_context=context))

    return session.get(url=url, timeout=60)


def validateData(data: dict, schema: dict) -> bool:
    try:
        validate(instance=data, schema=schema)
    except ValidationError as ve:
        print(ve)
        return False
    else:
        return True
