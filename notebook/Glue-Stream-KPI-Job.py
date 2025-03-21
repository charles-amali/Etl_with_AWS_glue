import sys
import boto3
import json
import decimal
import pandas as pd
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'DYNAMODB_KPIS'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


users_df = spark.read.csv(f"{args['S3_INPUT_PATH'].strip()}users/20250320_160749_users.csv", header=True, inferSchema=True)
songs_df = spark.read.csv(f"{args['S3_INPUT_PATH'].strip()}songs/20250320_160751_songs.csv", header=True, inferSchema=True)
streams_df = spark.read.csv(f"{args['S3_INPUT_PATH'].strip()}streams/20250320_160753_streams1.csv", header=True, inferSchema=True)


users_df = users_df.withColumn("date", F.to_date(F.col("created_at")))

streams_df= streams_df.withColumnRenamed("track_id", "track_id_1")


# Join streams with songs to get genre
data = songs_df.join(streams_df, songs_df.track_id == streams_df.track_id_1, 'inner') \
                  .join(users_df, streams_df.user_id == users_df.user_id, 'inner') \
                  .select(songs_df['*'],
                         streams_df.user_id.alias('stream_user_id'),
                         streams_df.track_id_1,
                         streams_df.listen_time,
                         users_df.user_id.alias('user_user_id'),
                         users_df.user_name,
                         users_df.user_age,
                         users_df.user_country,
                         users_df.created_at)

# Calculate Listen Count, Unique Listeners, Total Listening Time, and Avg Listening Time per User
kpi_base = data.groupBy("created_at", "track_genre").agg(
    F.count("track_id").alias("listen_count"),
    F.countDistinct("user_user_id").alias("unique_listeners"),
    F.sum("duration_ms").alias("total_listening_time")
).withColumn(
    "avg_listening_time_per_user",
    (F.col("total_listening_time") / F.when(F.col("unique_listeners") > 0, F.col("unique_listeners")).otherwise(1)) / 1000
)

# Compute Top 3 Songs per Genre per Day
song_listen_count = data.groupBy(
    "created_at", "track_genre", "track_name"
).agg(
    F.count("track_id").alias("listen_count")
)

song_rank_window = Window.partitionBy("created_at", "track_genre").orderBy(F.desc("listen_count"))

top_songs_per_genre = song_listen_count.withColumn("rank", F.rank().over(song_rank_window)) \
                                        .filter(F.col("rank") <= 3) \
                                        .groupBy("created_at", "track_genre") \
                                        .agg(F.concat_ws(", ", F.collect_list("track_name")).alias("top_3"))

# Compute Top 5 Genres per Day
genre_listen_count = data.groupBy("created_at", "track_genre").agg(
    F.count("track_id").alias("listen_count")
)

genre_rank_window = Window.partitionBy("created_at").orderBy(F.desc("listen_count"))

top_genres_per_day = genre_listen_count.withColumn("rank", F.rank().over(genre_rank_window)) \
                                       .filter(F.col("rank") <= 5) \
                                       .groupBy("created_at") \
                                       .agg(F.concat_ws(", ", F.collect_list("track_genre")).alias("top_5_genres"))

# Join all KPIs
final_kpis = kpi_base.join(top_songs_per_genre, ["created_at", "track_genre"], "left") \
                     .join(top_genres_per_day, ["created_at"], "left")

# Write directly to DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
# table = dynamodb.Table(args["DYNAMODB_KPIS"])
table = dynamodb.Table("Dynamodb-Kpis")


# Convert DataFrame to local Pandas 
final_kpis_pd = final_kpis.toPandas()

final_kpis_pd = final_kpis_pd.fillna(0)  # Replace NaN values with 0

for index, row in final_kpis_pd.iterrows():
    item = {
        "date": str(row["created_at"]),
        "track_genre": row["track_genre"],
        "listen_count": int(row["listen_count"]),
        "unique_listeners": int(row["unique_listeners"]),
        "total_listening_time": int(row["total_listening_time"]),
        "avg_listening_time_per_user": decimal.Decimal(str(row["avg_listening_time_per_user"])),
        "top_3_songs": row["top_3"] if row["top_3"] else "",
        "top_5_genres": row["top_5_genres"] if row["top_5_genres"] else ""
    }
    table.put_item(Item=item)


print("KPIs successfully loaded into DynamoDB.")

job.commit()