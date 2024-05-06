from spark_utilities import get_df_from_file
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from convert_csv_to_parquet import data_directory
from os.path import join

# Data file names
lap_times_filename: str = "lap_times"
race_results_filename: str = "results"
teammate_lap_delta_filename: str = "teammate_lap_deltas"

# Column names
race_id: str = "raceId"
driver_id: str = "driverId"
rival_id: str = "rivalId"
constructor_id: str = "constructorId"
laps: str = "laps"
lap: str = "lap"
delta_per_lap = "deltaPerLap"
milliseconds: str = "milliseconds"
delta_to_teammate_milliseconds: str = "deltaToTeammateMillis"


def get_race_teammate_rivals() -> DataFrame:
    """
    Generates a list of all drivers and their teammate rivals per race.

    Adds one column:
    - `rivalId`: the `driverId` of the rival teammate

    Resulting Dataframe has the following columns:
    - `raceId`
    - `constructorId`
    - `driverId`
    - `rivalId`

    :return: Dataframe with results (not persisted)
    """

    drivers_constructors_races = (
        get_df_from_file(race_results_filename)
        .select(col(race_id),
                col(driver_id),
                col(constructor_id))
        .sort(col(race_id), col(driver_id))
    )

    rival_driver_setup = (
        drivers_constructors_races
        .withColumnRenamed(driver_id, rival_id)
    )

    rival_drivers = (
        drivers_constructors_races
        .join(rival_driver_setup, [race_id, constructor_id])
        .where(col(driver_id) != col(rival_id))
    )

    return rival_drivers


def get_rival_race_time_deltas() -> DataFrame:
    """
    Calculates the time delta (milliseconds) between teammates per race.
    Sprint races are not considered.

    Adds two columns:
    - `rivalId`: the `driverId` of the rival teammate
    - `deltaToTeammateMillis`: the race time difference in milliseconds to the
                               rival teammate (negative = faster). Only
                               compares the laps completed by all teammates
                               for the same constructor for that race.

    Resulting Dataframe has the following columns:
    - `raceId`
    - `driverId`
    - `rivalId`
    - `constructorId`
    - `deltaToTeammateMillis`
    - `laps`

    :return: Dataframe with results (not persisted)
    """

    comparable_race_time_milliseconds = "comparableRaceTimeMillis"
    comparable_rival_race_time_milliseconds = "comparableRivalRaceTimeMillis"

    driver_laps_completed = (
        get_df_from_file(race_results_filename)
        .select(col(race_id),
                col(driver_id),
                col(constructor_id),
                col(laps))
    )

    min_laps_completed_per_constructor = (
        driver_laps_completed
        .groupBy(race_id, constructor_id)
        .min(laps)
    )

    comparable_driver_laps_completed = (
        driver_laps_completed
        .join(min_laps_completed_per_constructor, [race_id, constructor_id])
        .select(col(race_id),
                col(constructor_id),
                col(driver_id),
                col(f"min({laps})"))
        .withColumnRenamed(f"min({laps})", laps)
    )

    lap_times = (
        get_df_from_file(lap_times_filename)
        .select(col(race_id),
                col(driver_id),
                col(lap),
                col(milliseconds))
    )

    comparable_race_times = (
        lap_times
        .join(comparable_driver_laps_completed, [race_id, driver_id])
        .where(col(lap) <= col(laps))
        .select(col(race_id),
                col(driver_id),
                col(constructor_id),
                col(milliseconds))
        .groupBy(race_id, driver_id, constructor_id)
        .sum(milliseconds)
        .withColumnRenamed(
            f"sum({milliseconds})",
            comparable_race_time_milliseconds)
    )

    rival_comparable_race_times = (
        comparable_race_times
        .withColumnRenamed(driver_id, rival_id)
        .withColumnRenamed(comparable_race_time_milliseconds,
                           comparable_rival_race_time_milliseconds)
    )

    rival_drivers = get_race_teammate_rivals()

    delta_to_rival_milliseconds = (
        rival_drivers
        .join(comparable_race_times, [race_id, driver_id, constructor_id])
        .join(rival_comparable_race_times, [race_id, rival_id, constructor_id])
        .withColumn(delta_to_teammate_milliseconds,
                    col(comparable_race_time_milliseconds) -
                    col(comparable_rival_race_time_milliseconds))
        .select(col(race_id),
                col(driver_id),
                col(rival_id),
                col(constructor_id),
                col(delta_to_teammate_milliseconds))
        .join(comparable_driver_laps_completed, [race_id, constructor_id, driver_id])
    )

    return delta_to_rival_milliseconds


def write_teammate_lap_time_deltas() -> None:
    """
    Calculates and writes to parquet the mean lap time delta between teammates.

    Introduces one column:
    - `deltaPerLap`: the mean delta

    Resulting Dataframe has the following columns:
    - `driverId`
    - `rivalId`
    - `deltaPerLap`
    - `laps`
    """

    teammate_lap_time_deltas = (
        get_rival_race_time_deltas()
        .drop(race_id)
        .drop(constructor_id)
        .groupBy(driver_id, rival_id)
        .sum(delta_to_teammate_milliseconds, laps)
        .withColumn(delta_per_lap,
                    col(f"sum({delta_to_teammate_milliseconds})") / col(f"sum({laps})"))
        .drop(f"sum({delta_to_teammate_milliseconds})")
    )

    output_file = join(data_directory(), f"{teammate_lap_delta_filename}.parquet")

    teammate_lap_time_deltas.write.parquet(
        output_file,
        compression="zstd", mode="overwrite")


if __name__ == "__main__":
    write_teammate_lap_time_deltas()
