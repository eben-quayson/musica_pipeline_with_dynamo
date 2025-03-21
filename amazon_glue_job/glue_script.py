import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, avg, row_number, to_date, regexp_replace, when
from pyspark.sql.window import Window

# Get Glue job arguments
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "S3_INPUT_BUCKET",
    "DYNAMODB_GENRE_KPIS",
    "DYNAMODB_TOP_SONGS",
    "DYNAMODB_TOP_GENRES"
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Glue Job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# S3 Paths
S3_INPUT_PATH = f"s3://{args['S3_INPUT_BUCKET']}/processed_csvs/"

# DynamoDB Table Names
DYNAMODB_GENRE_KPIS = args["DYNAMODB_GENRE_KPIS"]
DYNAMODB_TOP_SONGS = args["DYNAMODB_TOP_SONGS"]
DYNAMODB_TOP_GENRES = args["DYNAMODB_TOP_GENRES"]

# ✅ Load CSVs as Spark DataFrames
users_df = spark.read.csv(f"{S3_INPUT_PATH}users.csv", header=True, inferSchema=True)
songs_df = spark.read.csv(f"{S3_INPUT_PATH}songs.csv", header=True, inferSchema=True)
streams_df = spark.read.csv(f"{S3_INPUT_PATH}streams.csv", header=True, inferSchema=True)

# Debug: Check initial schemas and row counts
print("\n🔍 Users Schema:")
users_df.printSchema()
print(f"Users Row Count: {users_df.count()}")

print("\n🔍 Songs Schema:")
songs_df.printSchema()
print(f"Songs Row Count: {songs_df.count()}")

print("\n🔍 Streams Schema:")
streams_df.printSchema()
print(f"Streams Row Count: {streams_df.count()}")

# Clean the track_genre column: Remove numbers and handle missing values
songs_df = songs_df.withColumn("track_genre", regexp_replace(col("track_genre"), r"\d+", ""))
songs_df = songs_df.withColumn(
    "track_genre",
    when(col("track_genre").isNull() | (col("track_genre") == "") | (col("track_genre") == "."), "Unknown")
    .otherwise(col("track_genre"))
)

# Convert listen_time to date (assuming format is 'M/d/yyyy H:mm')
streams_df = streams_df.withColumn("date", to_date(col("listen_time"), "M/d/yyyy H:mm"))

# Join streams with songs to get genre information
streams_songs_df = streams_df.join(songs_df, "track_id", "inner").select(
    col("user_id"), 
    col("track_id"), 
    col("track_genre").alias("genre"), 
    col("date"), 
    col("duration_ms")
).filter(col("genre") != "")  # Remove empty genres

# Debug: Check streams_songs_df schema and row count
print("\n🔍 Streams + Songs Schema:")
streams_songs_df.printSchema()
print(f"Streams + Songs Row Count: {streams_songs_df.count()}")

# **1️⃣ Compute Genre-Level KPIs**
genre_kpis = streams_songs_df.groupBy("date", "genre").agg(
    count("track_id").alias("listen_count"),
    countDistinct("user_id").alias("unique_listeners"),
    sum(col("duration_ms") / 60000).alias("total_listening_time"),  # Convert ms to minutes
    avg(col("duration_ms") / 60000).alias("avg_listening_time_per_user")
)

# Debug: Check genre_kpis schema and row count
print("\n🔍 Genre KPIs Schema:")
genre_kpis.printSchema()
print(f"Genre KPIs Row Count: {genre_kpis.count()}")

# **2️⃣ Compute Top 3 Songs per Genre per Day (No Ties)**
window_spec = Window.partitionBy("date", "genre").orderBy(col("listen_count").desc(), col("track_id"))

top_songs_per_genre = (
    streams_songs_df.groupBy("date", "genre", "track_id")
    .agg(count("track_id").alias("listen_count"))
    .withColumn("rank", row_number().over(window_spec)) 
    .orderBy("date", "genre", col("rank"))  # Ensure correct ordering
    .filter(col("rank") <= 3)
)

# Debug: Check top_songs_per_genre schema and row count
print("\n🔍 Top 3 Songs per Genre Schema:")
top_songs_per_genre.printSchema()
print(f"Top 3 Songs per Genre Row Count: {top_songs_per_genre.count()}")

# **3️⃣ Compute Top 5 Genres per Day**
window_genre = Window.partitionBy("date").orderBy(col("listen_count").desc())

top_genres_per_day = (
    genre_kpis.withColumn("rank", row_number().over(window_genre))  # Strict ranking (no ties)
    .filter(col("rank") <= 5)
    .orderBy("date", col("rank"))  # Ensure correct ordering
)

# Debug: Check top_genres_per_day schema and row count
print("\n🔍 Top 5 Genres per Day Schema:")
top_genres_per_day.printSchema()
print(f"Top 5 Genres per Day Row Count: {top_genres_per_day.count()}")


# ✅ Convert DataFrames to Glue DynamicFrames
genre_kpis_dyf = DynamicFrame.fromDF(genre_kpis, glueContext, "genre_kpis_dyf")
top_songs_dyf = DynamicFrame.fromDF(top_songs_per_genre, glueContext, "top_songs_dyf")
top_genres_dyf = DynamicFrame.fromDF(top_genres_per_day, glueContext, "top_genres_dyf")

# Print schemas of DynamicFrames
print("Genre KPIs DynamicFrame Schema:")

genre_kpis_dyf.printSchema()
print(f"Genre KPIS: {genre_kpis_dyf}")

print("Top Songs DynamicFrame Schema:")
top_songs_dyf.printSchema()

print("Top Genres DynamicFrame Schema:")
top_genres_dyf.printSchema()

# ✅ Write to DynamoDB
glueContext.write_dynamic_frame.from_options(
    frame=genre_kpis_dyf,
    connection_type="dynamodb",
    connection_options={"dynamodb.output.tableName": DYNAMODB_GENRE_KPIS}
)

glueContext.write_dynamic_frame.from_options(
    frame=top_songs_dyf,
    connection_type="dynamodb",
    connection_options={"dynamodb.output.tableName": DYNAMODB_TOP_SONGS}
)

glueContext.write_dynamic_frame.from_options(
    frame=top_genres_dyf,
    connection_type="dynamodb",
    connection_options={"dynamodb.output.tableName": DYNAMODB_TOP_GENRES}
)
print("📌 Checking Row Counts Before Writing to DynamoDB")

print("Genre KPIs row count:", genre_kpis.count())
print("Top Songs row count:", top_songs_per_genre.count())
print("Top Genres row count:", top_genres_per_day.count())

print("✅ Data successfully loaded into DynamoDB!")

# Commit the Glue job
job.commit()