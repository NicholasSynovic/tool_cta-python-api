import click
from pandas import DataFrame

from src.stops import Stops
from src.train import Arrivals, FollowThisTrain, Locations


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
    l: Locations = Locations(key=apiKey)

    stopsDF: DataFrame = s.get()
    print(stopsDF)

    stopID: str = stopsDF["stop_id"][0]

    arrivalsDF: DataFrame = a.get(stpid=stopID)
    print(arrivalsDF)

    rnID: str = arrivalsDF["rn"][0]

    fttDF: DataFrame = ftt.get(runnumber=rnID)
    print(fttDF)

    lDF: DataFrame = l.get(rt=["red", "blue"])
    print(lDF)


if __name__ == "__main__":
    main()
