from pyspark.sql.functions import col

def transform(df):
    df = df.withColumn("discounted_price", col("price") * 0.9)
    df = df.withColumn("tax", col("discounted_price") * 0.18)
    return df