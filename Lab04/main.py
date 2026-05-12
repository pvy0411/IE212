import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructField(
    StructField("Customer_Trx_ID", StringType, False),
    StructField("Subcriber_ID", StringType, False),
    StructField("Customer_Trx_ID", StringType, False),
)

customer_df = spark.read.format("csv").option(delimited=";", interSchema=True, header=True).load()

customer_df.printSchema()