from pyspark.sql import SparkSession 
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("EcommerceProject").getOrCreate()


spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
df = spark.read.parquet("/EcommerceData/ecommerce_final.parquet")

print("Schema of the dataset:")
df.printSchema()

print("Sample of 5 rows:")
df.show(5, truncate=False, vertical=True)

print("Checking for unknown brand and category_code values...")
no_unknown_brands=df.filter(F.col("brand") == "Unknown").count()
no_unknown_cat_code=df.filter(F.col("category_code") == "Unknown").count()
total = df.count()
print("the number of unknown brands: ",no_unknown_brands, "with percentage ",no_unknown_brands/total*100,"%")
print("the number of unknown category codes: ", no_unknown_cat_code, "with percentage ", no_unknown_cat_code/total*100, "%")


print("Checking for null values across all columns...")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

print("Checking for rows with price <= 0...")
zero_or_negative_price = df.filter(F.col("price") <= 0).count()
print("rows with price <= 0:", zero_or_negative_price)


print("Estimating approximate duplicate rows (HyperLogLog++)...")
approx_distinct = df.select(
    F.approx_count_distinct(F.concat_ws("||", *df.columns)).alias("approx_distinct")
).collect()[0]["approx_distinct"]
approx_duplicates = total - approx_distinct
print("approx distinct rows:", approx_distinct)
print("approx duplicate rows:", approx_duplicates)


print("Event type breakdown (funnel counts)...")
df.groupBy("event_type").count().show()


print("Calculating total revenue and average purchase price...")
df.filter(F.col("event_type") == "purchase").agg(
    F.sum("price").alias("total_revenue"),
    F.avg("price").alias("avg_purchase_price"),
    F.count("*").alias("num_purchases")
).show()



print("Calculating cart abandonment rate...")
session_summary = df.groupBy("user_session").agg(
    F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_adds"),
    F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")
)
abandoned = session_summary.filter((F.col("cart_adds") > 0) & (F.col("purchases") == 0)).count()
total_cart_sessions = session_summary.filter(F.col("cart_adds") > 0).count()
print("cart abandonment rate:", abandoned / total_cart_sessions * 100, "%")


print("Top 10 categories by purchase revenue...")
df.filter(F.col("event_type") == "purchase") \
  .groupBy("category_code") \
  .agg(F.sum("price").alias("revenue"), F.count("*").alias("purchases")) \
  .orderBy(F.desc("revenue")) \
  .show(10, truncate=False)


print("Event counts by hour of day, ordered busiest to quietest...")
spark.conf.set("spark.sql.session.timeZone", "UTC")

df_time = (df
    .withColumn("event_ts", (F.col("event_time") / 1000000000).cast("timestamp"))
    .withColumn("event_hour", F.hour("event_ts"))
    .withColumn("event_dow", F.dayofweek("event_ts")))

df_time.groupBy("event_hour").count().orderBy(F.desc("count")).show(24)
