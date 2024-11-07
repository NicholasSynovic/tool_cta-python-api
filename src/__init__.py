import ssl
from ssl import SSLContext

from requests import Response, Session
from requests.adapters import HTTPAdapter


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
