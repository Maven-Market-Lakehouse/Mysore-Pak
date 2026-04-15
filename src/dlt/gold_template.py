import dlt
from pyspark.sql import functions as F


@dlt.table(name="<gold_table_name>")
def gold_template():
    silver = dlt.read("<silver_table_name>")
    return silver.groupBy("<dimension_column>").agg(F.count("*").alias("row_count"))
