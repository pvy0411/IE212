from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_rdd = sc.textFile("users.txt")
ratings_rdd = sc.textFile("ratings_*.txt")
movies_rdd = sc.textFile("movies.txt")

def get_age_group(age: int) -> str:
    if age < 18: return "Under 18"
    elif age <= 30: return "18-30"
    elif age <= 40: return "31-40"
    elif age <= 50: return "41-50"
    else: return "51+"

def extract_user_age(data_row: str) -> tuple:
    try:
        components = data_row.split(",")
        userID = components[0]
        age = int(components[2])
        return userID, get_age_group(age)
    except:
        return None

users_mapped_rdd = users_rdd.map(extract_user_age).filter(lambda x: x is not None)

def extract_ratings_with_user(data_row: str) -> tuple:
    try:
        components = data_row.split(",")
        return components[0], (components[1], float(components[2]))
    except:
        return None

ratings_mapped_rdd = ratings_rdd.map(extract_ratings_with_user).filter(lambda x: x is not None)

rating_age_joined = ratings_mapped_rdd.join(users_mapped_rdd)

def map_to_movie_age_kv(data_row: tuple) -> tuple:
    userID, ((movieID, rating), age_group) = data_row
    return (movieID, age_group), (rating, 1)

movie_age_kv_rdd = rating_age_joined.map(map_to_movie_age_kv)

def reduce_movie_age_ratings(v1, v2):
    return (v1[0] + v2[0]), (v1[1] + v2[1])

reduced_movie_age = movie_age_kv_rdd.reduceByKey(reduce_movie_age_ratings)

def get_avg_movie_age(data_row):
    (movieID, age_group), (total_rating, total_count) = data_row
    avg = total_rating / total_count
    return movieID, (age_group, avg)

avg_movie_age_rdd = reduced_movie_age.map(get_avg_movie_age)

def extract_movie_title(data_row: str) -> tuple:
    try:
        components = data_row.split(",")
        return components[0], components[1]
    except:
        return None

movies_titles_rdd = movies_rdd.map(extract_movie_title).filter(lambda x: x is not None)

final_result_rdd = avg_movie_age_rdd.join(movies_titles_rdd)

print("\n" + "="*70)
print(f"{'Movie Title':<35} | {'Age Group':<12} | {'Avg Rating':<10}")
print("-" * 70)

for row in final_result_rdd.take(20):
    movieID, ((age_group, avg), title) = row
    print(f"{title[:33]:<35} | {age_group:<12} | {avg:.2f}")

spark.stop()