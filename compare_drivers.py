from functools import cache

from spark_utilities import get_df_from_file
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Dict, List

# Data file names
drivers_filename: str = "drivers"
lap_times_filename: str = "lap_times"
race_results_filename: str = "results"

# Column names
race_id: str = "raceId"
driver_id: str = "driverId"
rival_id: str = "rivalId"
constructor_id: str = "constructorId"
laps: str = "laps"
lap: str = "lap"
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


@cache
def get_all_driver_teammates() -> Dict[int, Dict[int, int]]:
    """
    Calculates all the teammates of a given `driverId`, and how many
    times they have been teammates.

    :return: Dict, indexed by `driverId`, value of Dict, indexed by teammate `driverId`,
             value of the number of times they have been teammates.
    """

    teammate_counts = (
        get_race_teammate_rivals()
        .drop(race_id)
        .drop(constructor_id)
        .groupBy(driver_id, rival_id)
        .count()
    )

    teammate_dictionary = {}

    teammate_counts_rows = [list(row) for row in teammate_counts.collect()]

    for row in teammate_counts_rows:
        if row[0] not in teammate_dictionary:
            teammate_dictionary[row[0]] = {}

        teammate_rivals = teammate_dictionary[row[0]]
        teammate_rivals[row[1]] = row[2]

    return teammate_dictionary


def teammate_paths(from_id: int, to_id: int, additional_depth=4) -> List[List[int]]:
    """
    Returns all paths in the relation graph of teammates (the `get_all_driver_teammates`
    dictionary) between two drivers, which have length `additional_depth` more than the
    shortest paths.

    Returns a list of paths, which are sequential lists of `driverId` teammates,
    ending in the `to_id` provided. Paths are guaranteed to be the same length.
    If no path exists, returns an empty list.

    :param from_id: the `driverId` of the origin teammate
    :param to_id: the `driverId` of the destination teammate
    :param additional_depth: the quantity to add to the shortest path length when calculating
                             maximum path depth
    :return: All shortest length paths between the two drivers
    """

    teammate_dictionary = get_all_driver_teammates()

    success_paths = []
    rival_missing = True

    if to_id in teammate_dictionary[from_id]:
        success_paths.append([[to_id]])
        rival_missing = False

    paths = []

    for teammate in teammate_dictionary[from_id].keys():
        paths.append([teammate])

    visited_teammates = set(teammate_dictionary[from_id].keys())
    visited_teammates.add(from_id)
    next_visited_teammates = set(visited_teammates)

    extra_depth_visits = 0

    while rival_missing or extra_depth_visits <= additional_depth:

        next_paths = []

        for path in paths:
            last_teammate = path[-1]

            if last_teammate == to_id:
                continue

            # Found a successful path to the query teammate at this depth
            if to_id in teammate_dictionary[last_teammate]:
                path.append(to_id)
                success_paths.append(path)
                rival_missing = False

            # Else add new paths to explore if not yet found a rival or reached max depth
            elif rival_missing or extra_depth_visits <= additional_depth:
                next_visited_teammates = next_visited_teammates.union(
                    set(teammate_dictionary[last_teammate].keys()))

                for next_teammate in teammate_dictionary[last_teammate].keys():
                    if next_teammate not in visited_teammates:
                        new_path = list(path)
                        new_path.append(next_teammate)
                        next_paths.append(new_path)

        paths = next_paths
        visited_teammates = next_visited_teammates
        if not rival_missing:
            extra_depth_visits += 1

    return success_paths


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
    )

    return delta_to_rival_milliseconds


# def get_mean_teammate_delta(id_one: int, id_two: int) -> float:
#     rival_race_time_deltas = get_rival_race_time_deltas().persist()


if __name__ == "__main__":
    # get_rival_race_time_deltas().show()
    # get_race_teammate_rivals().show()
    get_df_from_file(drivers_filename).show()
    print(teammate_paths(1, 4))
