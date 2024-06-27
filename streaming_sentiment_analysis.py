from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, explode, when, sum, count, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from textblob import TextBlob
import pyspark.sql.functions as F

def analyze_sentiment_textblob(title):
    if title is None:
        return 0.0
    analysis = TextBlob(title)
    return analysis.sentiment.polarity

def count_sentiments(df):
    return df.groupBy('destination') \
            .agg(
                F.sum(when(col('title_sentiment') > 0, 1).otherwise(0)).alias('positive'),
                F.sum(when(col('title_sentiment') < 0, 1).otherwise(0)).alias('negative'),
                F.sum(when(col('title_sentiment') == 0, 1).otherwise(0)).alias('neutral'),
                F.first('preprocessed_title').alias('title_count')
            )

def process_streaming_data(spark, schema1):
    streamingDF = spark.readStream \
        .format("json") \
        .schema(schema1) \
        .option("maxFilesPerTrigger", 1) \
        .load("/home/hdlmf/sparksql/Code/data/")

    analyze_sentiment_textblob_udf = udf(analyze_sentiment_textblob, DoubleType())
    sentiment_df = streamingDF.withColumn('title_sentiment', analyze_sentiment_textblob_udf('preprocessed_title'))
    exploded_df = sentiment_df.withColumn('destination', explode('destinations'))

    # Calculate the count of sentiments for each destination and sort by title count
    sentiment_counts_df = count_sentiments(exploded_df)
    return sentiment_counts_df

def main():
    spark = SparkSession \
        .builder \
        .appName("StreamingSentimentAnalysis") \
        .master("local[4]") \
        .getOrCreate()

    schema1 = StructType([
        StructField('preprocessed_title', StringType(), True),
        StructField('destinations', ArrayType(StringType()), True)
    ])

    processed_df = process_streaming_data(spark, schema1)

    query = processed_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()