from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

movies_rdd = sc.textFile("movies.txt")
ratings_rdd = sc.textFile("ratings_*.txt")

def removeMovieGenre(data_row: str) -> tuple[str]:
    components = data_row.split(",")
    movieID, movieTitle, _ = components
    return movieID, movieTitle
movies_rdd = movies_rdd.map(removeMovieGenre)

def removeUserIdTimestamp(data_row: str) -> tuple[str, float, int]:
    components = data_row.split(",")
    _, movieID, rating, _ = components
    return movieID, (float(rating), 1)
ratings_rdd = ratings_rdd.map(removeUserIdTimestamp)

def reduceRatings(value_1: tuple, value_2: tuple):
    rating_1, count_1 = value_1
    rating_2, count_2 = value_2
    return (rating_1 + rating_2), (count_1 + count_2)
reduced_ratings = ratings_rdd.reduceByKey(reduceRatings)

def getAvgRating(data_row: tuple):
    movieID, (total_ratings, total_count) = data_row
    avg_rating = total_ratings / total_count
    return movieID, (avg_rating, total_count)
reduced_ratings = reduced_ratings.map(getAvgRating)

title_ratings_rdd = reduced_ratings.join(movies_rdd)

for row in title_ratings_rdd.collect():
    _, value = row
    rating_count, movie_title = value
    avg_rating, count = rating_count
    print(f"{movie_title}: \n\t+Avg, rating: {avg_rating}\n\t+Times of rating: {count}")

spark.stop()