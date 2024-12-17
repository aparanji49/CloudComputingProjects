from pyspark.sql import SparkSession
import json
import re
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialization of SparkSession
sparkSess = SparkSession.builder \
    .appName("HashtagCount") \
    .getOrCreate()


# function to read data from input file
def read_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)


# Mapper function to return hashtags with a count of 1
def hashtag_mapper(json_data):
    mapped_data = []

    def extract_hashtags(data):
        if isinstance(data, dict):
            for value in data.values():
                extract_hashtags(value)
        elif isinstance(data, list):
            for item in data:
                extract_hashtags(item)
        elif isinstance(data, str):
            hashtags = re.findall(r"#\w+", data.lower())
            for hashtag in hashtags:
                mapped_data.append((hashtag, 1))

    extract_hashtags(json_data)
    return mapped_data


# JSON file path
file_path = 's3://p2-inputdata/smallTwitter.json'


# Load JSON data into an RDD
json_to_rdd = sparkSess.read.text(file_path).rdd.map(lambda row: row.value)

# Apply mapper function to each element of RDD
mapped_data = json_to_rdd.flatMap(hashtag_mapper)

# Sum of the counts
reduced_data = mapped_data.reduceByKey(lambda a, b: a + b)

# Displaying the top 20 hashtags
top_twenty_hashtags = reduced_data.takeOrdered(20, key=lambda x: -x[1])
output_str = "\n".join([f"{hashtag} {count}" for hashtag, count in top_twenty_hashtags])
print(output_str)

# Output path for the file
output_path = "s3://cloudcomputingvt/Aparanji/top_twenty_hashtags.txt"

# Write output to the file
sparkSess.sparkContext.parallelize([output_str]).saveAsTextFile(output_path)

# Stop the Spark context
sparkSess.stop()
