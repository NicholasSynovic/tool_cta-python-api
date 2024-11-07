import click
from pandas import DataFrame

from src.stops import Stops
from src.train import Arrivals


@click.command()
@click.option(
    "-k",
    "--key",
    "apiKey",
    required=True,
    type=str,
    help="CTA API key",
)
def main(apiKey: str) -> None:
    s: Stops = Stops()
    a: Arrivals = Arrivals(key=apiKey)

    stopsDF: DataFrame = s.get()

    stopID: str = stopsDF["stop_id"][0]

    df: DataFrame = a.get(stpid=stopID)
    print(df)


if __name__ == "__main__":
    main()
