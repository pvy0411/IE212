from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

users_rdd = sc.textFile("users.txt")
ratings_rdd = sc.textFile("ratings_*.txt")
movies_rdd = sc.textFile("movies.txt")

def extract_user_gender(data_row: str) -> tuple:
    components = data_row.split(",")
    userID = components[0]
    gender = components[1]
    return userID, gender

users_mapped_rdd = users_rdd.map(extract_user_gender)

def extract_ratings_with_user(data_row: str) -> tuple:
    components = data_row.split(",")
    userID = components[0]
    movieID = components[1]
    rating = float(components[2])
    return userID, (movieID, rating)

ratings_mapped_rdd = ratings_rdd.map(extract_ratings_with_user)

rating_gender_joined = ratings_mapped_rdd.join(users_mapped_rdd)

def map_to_movie_gender_kv(data_row: tuple) -> tuple:
    userID, ((movieID, rating), gender) = data_row
    return (movieID, gender), (rating, 1)

movie_gender_kv_rdd = rating_gender_joined.map(map_to_movie_gender_kv)

def reduce_movie_gender_ratings(v1, v2):
    return (v1[0] + v2[0]), (v1[1] + v2[1])

reduced_movie_gender = movie_gender_kv_rdd.reduceByKey(reduce_movie_gender_ratings)

def get_avg_movie_gender(data_row):
    (movieID, gender), (total_rating, total_count) = data_row
    avg = total_rating / total_count
    return movieID, (gender, avg)

avg_movie_gender_rdd = reduced_movie_gender.map(get_avg_movie_gender)

def extract_movie_title(data_row: str) -> tuple:
    components = data_row.split(",")
    return components[0], components[1] # (MovieID, Title)

movies_titles_rdd = movies_rdd.map(extract_movie_title)

final_result_rdd = avg_movie_gender_rdd.join(movies_titles_rdd)

for row in final_result_rdd.take(20):
    movieID, ((gender, avg), title) = row
    gender_str = "Male" if gender == "M" else "Female"
    print(f"Movie: {title} \n\t+Gender: {gender_str}\n\t+Avg rating: {avg:.2f}")

spark.stop()