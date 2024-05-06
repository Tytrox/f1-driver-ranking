from compare_drivers import compare_drivers, driver_id_to_reference
from spark_utilities import get_df_from_file
from pyspark.sql.functions import col
from functools import cmp_to_key
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


def drivers_comparator(id_one: int, id_two: int) -> int:
    score = compare_drivers(id_one, id_two)
    # print(f"{driver_id_to_reference(id_one)} vs {driver_id_to_reference(id_two)} : {score}")
    return int(score)


def sort_driver_ids(id_list: List[int]) -> None:
    id_list.sort(key=cmp_to_key(drivers_comparator))


if __name__ == "__main__":
    from random import shuffle
    ids = [1, 846, 830, 832, 4, 857, 852, 842, 839, 844, 840, 815,
           817, 858, 855, 848, 822, 847, 807, 825, ]

    final = []

    while len(ids) > 0:
        shuffle(ids)
        sort_driver_ids(ids)
        print(list(map(driver_id_to_reference, ids)))
        final.append(ids.pop(0))

    print(list(map(driver_id_to_reference, final)))
