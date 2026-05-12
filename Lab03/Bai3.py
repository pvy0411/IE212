from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_rdd = sc.textFile("users.txt")
ratings_rdd = sc.textFile("ratings_*.txt")
movies_rdd = sc.textFile("movies.txt")

def extract_user_gender(data_row):
    try:
        parts = data_row.split(",")
        return parts[0], parts[1]
    except: return None

user_gender_rdd = users_rdd.map(extract_user_gender).filter(lambda x: x is not None)

def extract_ratings(data_row):
    try:
        parts = data_row.split(",")
        return parts[0], (parts[1], float(parts[2]))
    except: return None

rating_user_rdd = ratings_rdd.map(extract_ratings).filter(lambda x: x is not None)

joined_user_rating = rating_user_rdd.join(user_gender_rdd)

def map_to_movie_gender_key(data):
    userid, ((movieid, rating), gender) = data
    return (movieid, gender), (rating, 1)

movie_gender_kv = joined_user_rating.map(map_to_movie_gender_key)

reduced_stats = movie_gender_kv.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

movie_gender_avg = reduced_stats.mapValues(lambda x: x[0] / x[1]) \
                                .map(lambda x: (x[0][0], (x[0][1], x[1])))

def extract_movie_name(data_row):
    try:
        parts = data_row.split(",")
        return parts[0], parts[1] # (MovieID, Title)
    except: return None

movie_names_rdd = movies_rdd.map(extract_movie_name).filter(lambda x: x is not None)

final_result = movie_gender_avg.join(movie_names_rdd)

print("\n" + "="*50)
print(f"{'Movie Title':<30} | {'Gender':<7} | {'Avg Rating':<10}")
print("-" * 50)

for row in final_result.take(20):
    movieid, ((gender, avg), title) = row
    gender_full = "Male" if gender == "M" else "Female"
    print(f"{title[:30]:<30} | {gender_full:<7} | {avg:.2f}")

spark.stop()