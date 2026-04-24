from pyspark.sql.functions import col, coalesce, lit

def transform(df):
    df = df.withColumn("price", coalesce(col("price"), lit(0)))
    df = df.withColumn("discounted_price", col("price") * 0.9)
    df = df.withColumn("tax", col("discounted_price") * 0.18)
    return df