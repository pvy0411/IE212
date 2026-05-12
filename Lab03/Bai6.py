from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

ratings_rdd = sc.textFile("ratings_*.txt")

def timestamp_to_year(unix_timestamp: str) -> int:
    try:
        return datetime.fromtimestamp(int(unix_timestamp)).year
    except:
        return 0

def extract_year_rating(data_row: str) -> tuple:
    try:
        components = data_row.split(",")
        rating = float(components[2])
        year = timestamp_to_year(components[3])
        
        if year == 0: return None
        return year, (rating, 1)
    except:
        return None

ratings_mapped_rdd = ratings_rdd.map(extract_year_rating).filter(lambda x: x is not None)

def reduce_year_ratings(v1: tuple, v2: tuple) -> tuple:
    rating_1, count_1 = v1
    rating_2, count_2 = v2
    return (rating_1 + rating_2), (count_1 + count_2)

reduced_year_ratings = ratings_mapped_rdd.reduceByKey(reduce_year_ratings)

def get_avg_year_rating(data_row: tuple) -> tuple:
    year, (total_ratings, total_count) = data_row
    avg_rating = total_ratings / total_count
    return year, (avg_rating, total_count)

final_year_averages_rdd = reduced_year_ratings.map(get_avg_year_rating)

print("\n" + "="*50)
print(f"{'Year':<10} | {'Avg Rating':<12} | {'Total Reviews'}")
print("-" * 50)

sorted_results = final_year_averages_rdd.sortByKey()

for row in sorted_results.collect():
    year, (avg, count) = row
    print(f"{year:<10} | {avg:.2f}        | {count}")

spark.stop()