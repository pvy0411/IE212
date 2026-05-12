from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_rdd = sc.textFile("users.txt")
ratings_rdd = sc.textFile("ratings_*.txt")

def extract_user_occupation(data_row: str) -> tuple:
    try:
        components = data_row.split(",")
        userID = components[0]
        occupation = components[3]
        return userID, occupation
    except:
        return None

users_mapped_rdd = users_rdd.map(extract_user_occupation).filter(lambda x: x is not None)

def extract_ratings_for_occ(data_row: str) -> tuple:
    try:
        components = data_row.split(",")
        userID = components[0]
        rating = float(components[2])
        return userID, (rating, 1)
    except:
        return None

ratings_mapped_rdd = ratings_rdd.map(extract_ratings_for_occ).filter(lambda x: x is not None)

rating_occ_joined = ratings_mapped_rdd.join(users_mapped_rdd)

def map_to_occ_kv(data_row: tuple) -> tuple:
    userID, ((rating, count), occupation) = data_row
    return occupation, (rating, count)

occ_kv_rdd = rating_occ_joined.map(map_to_occ_kv)

def reduce_occ_ratings(v1, v2):
    return (v1[0] + v2[0]), (v1[1] + v2[1])

reduced_occ_ratings = occ_kv_rdd.reduceByKey(reduce_occ_ratings)

def get_avg_occ_rating(data_row: tuple) -> tuple:
    occupation, (total_ratings, total_count) = data_row
    avg_rating = total_ratings / total_count
    return occupation, (avg_rating, total_count)

final_occ_averages_rdd = reduced_occ_ratings.map(get_avg_occ_rating)

print("\n" + "="*50)
print(f"{'Occupation':<20} | {'Avg Rating':<12} | {'Total Reviews'}")
print("-" * 50)

sorted_results = final_occ_averages_rdd.sortBy(lambda x: x[1][1], ascending=False)

for row in sorted_results.collect():
    occ, (avg, count) = row
    print(f"{occ:<20} | {avg:.2f}        | {count}")

spark.stop()