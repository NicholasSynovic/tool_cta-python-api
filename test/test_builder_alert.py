import pytest

from cta._builder.alert import AlertAPIBuilder


def test_AlertAPIBuilder() -> None:
    aab: AlertAPIBuilder = AlertAPIBuilder()
    assert aab.outputType == "json"

    aabXML: AlertAPIBuilder = AlertAPIBuilder(outputType="xml")
    assert aabXML.outputType == "xml"

    with pytest.raises(expected_exception=ValueError):
        AlertAPIBuilder(outputType="test")


def test_buildRouteStatusAPIURL() -> None:
    aab: AlertAPIBuilder = AlertAPIBuilder()
    url: str = "http://www.transitchicago.com/api/1.0/routes.aspx?outputType=json"

    assert aab.buildRouteStatusAPIURL() == url

    assert aab.buildRouteStatusAPIURL(_type="bus") == f"{url}&type=bus"
    assert aab.buildRouteStatusAPIURL(_type=["bus", "rail"]) == f"{url}&type=bus%2Crail"

    with pytest.raises(ValueError):
        aab.buildRouteStatusAPIURL(_type="test")
        aab.buildRouteStatusAPIURL(_type=["test"])

    with pytest.raises(TypeError):
        aab.buildRouteStatusAPIURL(_type=1)
