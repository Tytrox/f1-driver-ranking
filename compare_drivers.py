from spark_utilities import get_df_from_file
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# Data file names
drivers_filename = "drivers"
lap_times_filename = "lap_times"
race_results_filename = "results"

# Column names
race_id = "raceId"
driver_id = "driverId"
rival_id = "rivalId"
constructor_id = "constructorId"
laps = "laps"
lap = "lap"
milliseconds = "milliseconds"
delta_to_teammate_milliseconds = "deltaToTeammateMillis"


def get_rival_race_time_deltas() -> DataFrame:
    """
    Calculates the time delta (milliseconds) between teammates per race.

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

    drivers_constructors_races = (
        comparable_race_times
        .select(col(race_id),
                col(driver_id),
                col(constructor_id))
        .groupBy(race_id, driver_id, constructor_id)
        .count()
        .drop("count")
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
    )

    return delta_to_rival_milliseconds


if __name__ == "__main__":
    get_rival_race_time_deltas().show()
