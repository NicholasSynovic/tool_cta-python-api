import ssl

import requests
import requests.adapters


class SSLAdapter(requests.adapters.HTTPAdapter):
    """
    SSLAdapter _summary_

    _extended_summary_

    :param requests: _description_
    :type requests: _type_
    """

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)


class RequestHandler:
    def __init__(self) -> None:
        self.sslAdapter: SSLAdapter = SSLAdapter()

    def get(self, url: str) -> requests.Response:
        """
        get _summary_

        _extended_summary_

        :param url: _description_
        :type url: str
        :return: _description_
        :rtype: requests.Response
        """
        context: ssl.SSLContext = ssl.create_default_context()
        context.set_ciphers("DEFAULT:@SECLEVEL=1")

        session: requests.Session = requests.Session()
        session.mount("https://", self.sslAdapter(ssl_context=context))

        return session.get(url=url, timeout=60)
