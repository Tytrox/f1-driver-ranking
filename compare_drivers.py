import numpy as np

from functools import cache, lru_cache
from spark_utilities import get_df_from_file
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Dict, List, Optional, Tuple

# Data file names
drivers_filename: str = "drivers"
lap_times_filename: str = "lap_times"
race_results_filename: str = "results"
races_filename: str = "races"

# Column names
race_id: str = "raceId"
driver_id: str = "driverId"
rival_id: str = "rivalId"
constructor_id: str = "constructorId"
laps: str = "laps"
lap: str = "lap"
number: str = "number"
driver_ref = "driverRef"
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
             value of the number of laps they have been teammates.
    """

    teammate_lap_counts = (
        get_rival_race_time_deltas()
        .drop(race_id)
        .drop(constructor_id)
        .groupBy(driver_id, rival_id)
        .sum(laps)
    )

    teammate_dictionary = {}

    teammate_counts_rows = [list(row) for row in teammate_lap_counts.collect()]

    for row in teammate_counts_rows:
        if row[0] not in teammate_dictionary:
            teammate_dictionary[row[0]] = {}

        teammate_rivals = teammate_dictionary[row[0]]
        teammate_rivals[row[1]] = row[2]

    return teammate_dictionary


def teammate_paths(from_id: int, to_id: int, additional_depth=1) -> List[List[int]]:
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

    if from_id not in teammate_dictionary:
        raise Exception(f"`{from_id}` is not a valid `driverId`")

    if to_id not in teammate_dictionary:
        raise Exception(f"`{to_id}` is not a valid `driverId`")

    success_paths = []
    rival_missing = True

    if to_id in teammate_dictionary[from_id]:
        success_paths.append([to_id])
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


@lru_cache
def mean_direct_teammate_lap_delta(id_one: int, id_two: int) -> Optional[Tuple[float, float]]:
    """
    Calculates the number of laps the two drivers were direct teammates and the mean delta per
    lap (in milliseconds) between them.

    Mean delta negative if `id_one` is faster on average than `id_two`.
    Returns `None` if the two ids have never been direct teammates.

    :param id_one: the `driverId` of the first teammate
    :param id_two: the `driverId` of the second teammate
    :return: A tuple of the number of laps both teammates completed together and the mean lap
             delta between teammates, or `None` if they weren't teammates
    """

    delta_per_lap = "deltaPerLap"

    rival_race_time_deltas = (
        get_rival_race_time_deltas()
        .drop(race_id)
        .drop(constructor_id)
        .groupBy(driver_id, rival_id)
        .sum(delta_to_teammate_milliseconds, laps)
        .withColumn(delta_per_lap,
                    col(f"sum({delta_to_teammate_milliseconds})") / col(f"sum({laps})"))
        .drop(f"sum({delta_to_teammate_milliseconds})")
        .persist()
    )

    delta_row = (
        rival_race_time_deltas
        .where((col(driver_id) == id_one) & (col(rival_id) == id_two))
    )

    collected_rows = delta_row.collect()

    if len(collected_rows) < 1:
        return None

    first_row = list(collected_rows[0])

    if len(first_row) < 4:
        raise Exception("Not enough columns")

    return first_row[2], first_row[3]


def compare_drivers(
        id_one: int,
        id_two: int,
        depth_factor=1,
        indirection_penalty=0.25) -> Optional[float]:
    """
    Compares two drivers by calculating a weighted average of mean racing lap time delta between
    shared teammates.

    :param id_one: the `driverId` of the first driver
    :param id_two: the `driverId` of the second driver
    :param depth_factor: the limit of extra nodes to search
    :param indirection_penalty: the multiplier to de-value longer teammate chains
    :return: A weighted average of mean lap-time deltas between teammates, or `None` if there is
             no common teammate
    """

    paths = teammate_paths(id_one, id_two, depth_factor)
    mean_deltas = []
    lap_scores = []

    if len(paths) < 1:
        return None

    min_path_length = min(map(len, paths))

    for path in paths:

        path = list(path)

        mean_path_delta = 0
        lap_score = 0
        indirection_penalty_multiplier = 1  # The multiplier penalty applied to lap score

        previous_teammate = id_one

        i = 0

        while len(path) > 0:
            i += 1
            next_teammate = path.pop(0)

            if next_teammate is not id_two or i > min_path_length:
                indirection_penalty_multiplier *= indirection_penalty

            next_lap_score, next_delta = (
                mean_direct_teammate_lap_delta(
                    previous_teammate,
                    next_teammate)
            )

            mean_path_delta += next_delta
            lap_score += next_lap_score

            previous_teammate = next_teammate

        mean_deltas.append(mean_path_delta)
        lap_scores.append(lap_score * indirection_penalty_multiplier)

    print(mean_deltas)
    print(lap_scores)

    weighted_average_delta = np.average(mean_deltas, weights=lap_scores)
    return weighted_average_delta


def driver_reference_to_id(driver_reference: str) -> Optional[int]:
    """
    Looks up the `driverId` from `driverRef`

    :param driver_reference: `driverRef`
    :return: first matching `driverId` if present, or None
    """
    driver = (
        get_df_from_file(drivers_filename)
        .persist()
        .where(col(driver_ref) == driver_reference)
    )

    rows = driver.collect()

    if len(rows) < 1:
        return None

    return rows[0].asDict()[driver_id]


def driver_id_to_reference(id: int) -> Optional[str]:
    """
    Looks up the `driverRef` from `driverId`

    :param id: `driverId`
    :return: first matching `driverRef` if present, or None
    """
    driver = (
        get_df_from_file(drivers_filename)
        .persist()
        .where(col(driver_id) == id)
    )

    rows = driver.collect()

    if len(rows) < 1:
        return None

    return rows[0].asDict()[driver_ref]


if __name__ == "__main__":
    # get_rival_race_time_deltas().show()
    # get_race_teammate_rivals().show()
    # for row in get_df_from_file(drivers_filename).collect():
    #     print(row.asDict())

    from_number = "norris"
    to_number = "sainz"
    depth_extension = 2

    print(teammate_paths(driver_reference_to_id(from_number), driver_reference_to_id(to_number),
                         additional_depth=depth_extension))
    print(compare_drivers(driver_reference_to_id(from_number), driver_reference_to_id(to_number),
                          depth_factor=depth_extension))

    print("done")
    # get_df_from_file("races").show()
