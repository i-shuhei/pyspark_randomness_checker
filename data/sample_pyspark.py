from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def sample(
    df: DataFrame, keys: List[str], groupby_keys: List[str], orderby_keys: List[str]
):
    # drop duplicates
    df = df.dropDuplicates(subset=keys)
    df = df.dropDuplicates(keys)

    # rank function
    window = Window.partitionBy(groupby_keys).orderBy(orderby_keys)
    df = df.withColumn("row_number", F.row_number().over(window))

    # impute quantiles
    df = df.approxQuantile("x", [0.5], 0.25)
    df = df.approxQuantile("x", [0.5], 0)
    return df
