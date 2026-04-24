from pyspark.sql.functions import col

def transform(df):

    return df.withColumn("discounted_price", col("price") * 0.9)