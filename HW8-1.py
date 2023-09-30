from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TopRatedMovies").getOrCreate()

rating_file_path = "gs://dataprocbucket_new/rating.csv"

ratings_df = spark.read.csv(rating_file_path, header=True, inferSchema=True)

movie_rating_counts = ratings_df.groupBy("movieID").count()

top_rated_movies = movie_rating_counts.orderBy("count", ascending=False).limit(20)
top_rated_movies.show()

spark.stop()