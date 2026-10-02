from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (SparkSession.builder
         .appName("HealthcareProject")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


# data loading
df = spark.read.format("avro").load("/HealthcareData/healthcare_dataset.avro")

# data inspection
total = df.count()
print("========DATASET SIZE========")
print("rows:", total, "| columns:", len(df.columns))

print("========SCHEMA========")
df.printSchema()

print("========SAMPLE DATA (first 5 rows)========")
df.show(5)

# data quality checks
print("========NULL VALUES PER COLUMN========")
df.select([F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).show(vertical=True)

print("========DUPLICATE ROWS========")
cleaned_df = df.dropDuplicates()
print("number of duplicate rows:", total - cleaned_df.count())

# statistic summary
print("========STATISTICS SUMMARY (after removing duplicates)========")
cleaned_df.describe().show()


print("========SAVING AS TABLE========")
spark.sql("CREATE DATABASE IF NOT EXISTS healthcare")

(cleaned_df.write
 .mode("overwrite")
 .format("parquet")
 .saveAsTable("healthcare.admissions")
 )

print("========TABLES IN HEALTHCARE DATABASE========")
spark.sql("SHOW TABLES IN healthcare").show()

print("========ROW COUNT CHECK========")
spark.sql("SELECT COUNT(*) AS total_rows FROM healthcare.admissions").show()


# ---------- analytical queries ----------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW valid_admissions AS
    SELECT *, DATEDIFF(Discharge_Date, Date_of_Admission) AS Length_of_Stay
    FROM healthcare.admissions
    WHERE Billing_Amount > 0
      AND Discharge_Date >= Date_of_Admission
""")

print("========OVERALL KPIs========")
spark.sql("""
    SELECT COUNT(*) AS total_admissions,
           ROUND(SUM(Billing_Amount), 0) AS total_revenue,
           ROUND(AVG(Billing_Amount), 2) AS avg_bill,
           ROUND(AVG(Length_of_Stay), 1) AS avg_stay_days
    FROM valid_admissions
""").show()

print("========ADMISSIONS, AVG BILL AND AVG STAY PER MEDICAL CONDITION========")
spark.sql("""
    SELECT Medical_Condition,
           COUNT(*) AS admissions,
           ROUND(AVG(Billing_Amount), 2) AS avg_bill,
           ROUND(AVG(Length_of_Stay), 1) AS avg_stay_days
    FROM valid_admissions
    GROUP BY Medical_Condition
    ORDER BY avg_bill DESC
""").show()

print("========REVENUE PER INSURANCE PROVIDER========")
spark.sql("""
    SELECT Insurance_Provider,
           COUNT(*) AS admissions,
           ROUND(SUM(Billing_Amount), 0) AS total_revenue,
           ROUND(AVG(Billing_Amount), 2) AS avg_bill
    FROM valid_admissions
    GROUP BY Insurance_Provider
    ORDER BY total_revenue DESC
""").show()

print("========ADMISSION TYPE: COUNT, AVG BILL, AVG STAY========")
spark.sql("""
    SELECT Admission_Type,
           COUNT(*) AS admissions,
           ROUND(AVG(Billing_Amount), 2) AS avg_bill,
           ROUND(AVG(Length_of_Stay), 1) AS avg_stay_days
    FROM valid_admissions
    GROUP BY Admission_Type
    ORDER BY admissions DESC
""").show()

print("========AGE GROUP ANALYSIS========")
spark.sql("""
    SELECT CASE WHEN Age < 18 THEN '0-17'
                WHEN Age < 40 THEN '18-39'
                WHEN Age < 65 THEN '40-64'
                ELSE '65+' END AS age_group,
           COUNT(*) AS admissions,
           ROUND(AVG(Billing_Amount), 2) AS avg_bill,
           ROUND(AVG(Length_of_Stay), 1) AS avg_stay_days
    FROM valid_admissions
    GROUP BY 1
    ORDER BY 1
""").show()

print("========YEARLY TREND========")
spark.sql("""
    SELECT YEAR(Date_of_Admission) AS year,
           COUNT(*) AS admissions,
           ROUND(SUM(Billing_Amount), 0) AS total_revenue
    FROM valid_admissions
    GROUP BY 1
    ORDER BY 1
""").show()

print("========PERCENTAGE OF ABNORMAL TEST RESULTS PER MEDICATION========")
spark.sql("""
    SELECT Medication,
           COUNT(*) AS admissions,
           ROUND(100 * SUM(CASE WHEN Test_Results = 'Abnormal' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_abnormal
    FROM valid_admissions
    GROUP BY Medication
    ORDER BY pct_abnormal DESC
""").show()

print("========TOP 3 CONDITIONS BY REVENUE IN EACH AGE GROUP========")
spark.sql("""
    SELECT * FROM (
        SELECT CASE WHEN Age < 40 THEN 'under 40'
                    WHEN Age < 65 THEN '40-64'
                    ELSE '65+' END AS age_group,
               Medical_Condition,
               ROUND(SUM(Billing_Amount), 0) AS revenue,
               RANK() OVER (PARTITION BY CASE WHEN Age < 40 THEN 'under 40'
                                              WHEN Age < 65 THEN '40-64'
                                              ELSE '65+' END
                            ORDER BY SUM(Billing_Amount) DESC) AS rnk
        FROM valid_admissions
        GROUP BY 1, 2
    )
    WHERE rnk <= 3
    ORDER BY age_group, rnk
""").show()
