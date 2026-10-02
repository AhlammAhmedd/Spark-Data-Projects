from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SupplyChainProject").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("/data/DataCoSupplyChainDataset.csv", header=True, inferSchema=True)
df.printSchema()
total = df.count()
print("total rows:", total)

print("Sample of 5 rows:")
df.show(5)

print("Checking for null values across all columns...")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

print("Checking for invalid (non-positive) sales, price, and quantity values...")
df.filter(F.col("Sales") <= 0).count()
df.filter(F.col("Product Price") <= 0).count()
df.filter(F.col("Order Item Quantity") <= 0).count()


print("Delivery status breakdown...")
df.groupBy("Delivery Status").count().orderBy(F.desc("count")).show()

print("Delivery status breakdown...")
df.groupBy("Delivery Status").count().orderBy(F.desc("count")).show()

print("Late delivery rate by order region...")
df.groupBy("Order Region").agg(
    F.avg("Late_delivery_risk").alias("late_delivery_rate"),
    F.count("*").alias("num_orders")
).orderBy(F.desc("late_delivery_rate")).show(20, truncate=False)

print("Top 10 categories by total sales...")
df.groupBy("Category Name").agg(
    F.sum("Sales").alias("total_sales"),
    F.sum("Order Item Quantity").alias("total_quantity")
).orderBy(F.desc("total_sales")).show(10, truncate=False)

print("Monthly sales trend...")
df_time = df.withColumn(
    "order_month",
    F.date_format(F.to_timestamp(F.col("order date (DateOrders)"), "M/d/yyyy H:mm"), "yyyy-MM")
)
df_time.groupBy("order_month").agg(F.sum("Sales").alias("monthly_sales")) \
    .orderBy("order_month").show(50, truncate=False)

