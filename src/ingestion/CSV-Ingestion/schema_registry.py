from pyspark.sql.types import *

SCHEMAS = {

    "transactions": StructType([
        StructField("transaction_date", StringType(), True),
        StructField("stock_date", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True)
    ]),

    "returns": StructType([
        StructField("return_date", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True)
    ]),

    "stores": StructType([
        StructField("store_id", IntegerType(), True),
        StructField("region_id", IntegerType(), True),
        StructField("store_type", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("store_city", StringType(), True),
        StructField("store_country", StringType(), True)
    ]),

    "regions": StructType([
        StructField("region_id", IntegerType(), True),
        StructField("sales_district", StringType(), True),
        StructField("sales_region", StringType(), True)
    ]),

    "calendar": StructType([
        StructField("date", StringType(), True)
    ])
}