from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round

spark = SparkSession.builder.appName("TopRatedMovies").getOrCreate()

rating_file_path = "gs://dataprocbucket_new/rating.csv"
ratings_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(rating_file_path)

movie_avg_rating = ratings_df.groupBy("movieID").agg(round(avg("rating"), 2).alias("avg_rating"), count("*").alias("num_ratings"))

top_rated_movies = movie_avg_rating.filter(col("num_ratings") >= 20).orderBy(col("avg_rating").desc()).limit(20)

top_rated_movies.select("movieID", "num_ratings", "avg_rating").show()

spark.stop()