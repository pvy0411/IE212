import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType
from pyspark.sql.functions import year, month, col, avg, count, expr, sum, round, countDistinct

spark = SparkSession.builder.getOrCreate()

# customer_schema = StructType([
#     StructField("Customer_Trx_ID", StringType(), False),
#     StructField("Subcriber_ID", StringType(), False),
#     StructField("Subcriber_Date", DataType(), False),
#     StructField("First_Order_Date", DataType(), False),
#     StructField("Customer_Postal_Code", StringType(), False),
#     StructField("Customer_City", StringType(), False),
#     StructField("Customer_Country", StringType(), False),
#     StructField("Customer_Country_Code", StringType(), False),
#     StructField("Age", IntegerType(), False),
#     StructField("Gender", StringType(), False),
# ])

# Câu 1
customers_df = spark.read.\
    format("csv").\
    options(delimiter=";", inferSchema=True, header=True).\
    load("hdfs://localhost:9000/customers/Customer_List.csv")

items_df = spark.read.\
    format("csv").\
    options(delimiter=";", inferSchema=True, header=True).\
    load("hdfs://localhost:9000/items/Order_Items.csv")

reviews_df = spark.read.\
    format("csv").\
    options(delimiter=";", inferSchema=True, header=True).\
    load("hdfs://localhost:9000/reviews/Order_Reviews.csv")

orders_df = spark.read.\
    format("csv").\
    options(delimiter=";", inferSchema=True, header=True).\
    load("hdfs://localhost:9000/orders/Orders.csv")

products_df = spark.read.\
    format("csv").\
    options(delimiter=";", inferSchema=True, header=True).\
    load("hdfs://localhost:9000/products/Products.csv")


# Câu 2
print("Câu 2")
print("Total orders:", orders_df.count())
print("Total customers:", orders_df.select("Customer_Trx_ID").distinct().count())
print("Total sellers:", items_df.select("Seller_ID").distinct().count())

# Câu 3
print("Câu 3")
orders_countries_df = orders_df.join(customers_df, "Customer_Trx_ID").select("Order_ID", "Customer_Country")
orders_countries_count = orders_countries_df.groupBy("Customer_Country").count()

sorted_orders_countries_count = orders_countries_count.orderBy("count", ascending = False)

for row in sorted_orders_countries_count.collect():
    print(row)

# Câu 4
print("Câu 4")
orders_time_df = orders_df.withColumn("Order_Year", year(col("Order_Purchase_Timestamp"))) \
                            .withColumn("Order_Month", month(col("Order_Purchase_Timestamp")))
orders_by_time = orders_time_df.groupBy("Order_Year", "Order_Month").count()
sorted_orders_by_time = orders_by_time.orderBy(col("Order_Year").asc(), col("Order_Month").desc())

for row in sorted_orders_by_time.collect():
    print(row)

# Câu 5
print("Câu 5")
clean_reviews_df = reviews_df.withColumn("Review_Score_Clean", expr("try_cast(Review_Score as int)"))
valid_reviews_df = clean_reviews_df.filter(
    (col("Review_Score_Clean").isNotNull()) & 
    (col("Review_Score_Clean") >= 1) & 
    (col("Review_Score_Clean") <= 5)
)

review_counts = valid_reviews_df.groupBy("Review_Score_Clean").count().orderBy("Review_Score_Clean")

average_score = valid_reviews_df.select(avg("Review_Score_Clean")).collect()[0][0]

print(f"Average Review Score: {average_score:.2f}")
for row in review_counts.collect():
    print(f"Score {row['Review_Score_Clean']}: {row['count']} reviews")

# Câu 6
print("Câu 6")
orders_2024_df = orders_df.filter(year(col("Order_Purchase_Timestamp")) == 2024)
joined_df = orders_2024_df.join(items_df, "Order_ID") \
                          .join(products_df, "Product_ID")
revenue_df = joined_df.withColumn("Revenue", col("Price") + col("Freight_Value"))
category_revenue_df = revenue_df.groupBy("Product_Category_Name") \
                                .agg(round(sum("Revenue"), 2).alias("Total_Revenue")) \
                                .orderBy(col("Total_Revenue").desc())
for row in category_revenue_df.collect():
    print(row)

# Câu 7
print("Câu 7")
items_reviews_df = items_df.join(clean_reviews_df, "Order_ID")
product_stats_df = items_reviews_df.groupBy("Product_ID") \
    .agg(
        count("Product_ID").alias("Total_Sold"),
        round(avg("Review_Score_Clean"), 2).alias("Average_Score")
    ) \
    .orderBy(col("Total_Sold").desc())

for row in product_stats_df.collect():
    print(row)

# Câu 10
print("Câu 10")
seller_revenue_df = items_df.withColumn("Item_Revenue", col("Price") + col("Freight_Value"))
seller_stats_df = seller_revenue_df.groupBy("Seller_ID") \
    .agg(
        round(sum("Item_Revenue"), 2).alias("Total_Revenue"),
        countDistinct("Order_ID").alias("Total_Orders")
    ) \
    .orderBy(col("Total_Revenue").desc(), col("Total_Orders").desc())

for row in seller_stats_df.collect():
    print(row)


