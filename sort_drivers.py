from numpy import mean

from compare_drivers import compare_drivers, driver_id_to_reference
from spark_utilities import get_df_from_file
from pyspark.sql.functions import col
from typing import List

# Data file names
drivers_filename: str = "drivers"

# Column names
driver_id: str = "driverId"


def fetch_all_driver_ids() -> List[int]:
    drivers = (
        get_df_from_file(drivers_filename)
        .select(col(driver_id))
        .collect()
    )

    return [list(row)[0] for row in drivers]


def sort_driver_ids(id_list: List[int]) -> None:
    for _ in range(3):
        for i in range(len(id_list), 1, -1):
            for j in range(0, i - 1):
                if compare_drivers(id_list[j], id_list[j + 1]) > 0:
                    id_list[j], id_list[j + 1] = id_list[j + 1], id_list[j]


if __name__ == "__main__":
    from random import shuffle

    ids = [1, 846, 830, 832, 4, 857, 852, 842, 839, 844, 840, 815,
           817, 858, 855, 848, 822, 847, 807, 825]

    position_lists = {}

    for _ in range(100):
        shuffle(ids)
        sort_driver_ids(ids)

        for i, driver in enumerate(ids):
            if driver not in position_lists:
                position_lists[driver] = []

            position_lists[driver].append(i)

    mean_positions = []

    for driver in position_lists.keys():
        mean_positions.append((driver, mean(position_lists[driver])))

    mean_positions.sort(key=lambda x: x[1])

    print(list(map(lambda x: driver_id_to_reference(x[0]), mean_positions)))
    print(list(map(lambda x: x[1], mean_positions)))
