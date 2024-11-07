import click
from pandas import DataFrame

from src.stops import Stops
from src.train import Arrivals, FollowThisTrain


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
    ftt: FollowThisTrain = FollowThisTrain(key=apiKey)

    stopsDF: DataFrame = s.get()
    print(stopsDF)

    stopID: str = stopsDF["stop_id"][0]

    arrivalsDF: DataFrame = a.get(stpid=stopID)
    print(arrivalsDF)

    fttDF: DataFrame = ftt.get(runnumber=316)
    print(fttDF)


if __name__ == "__main__":
    main()
