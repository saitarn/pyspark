from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("lesson3 operation") \
        .getOrCreate()

data_file = '/Users/macbookpro/Desktop/learning/python/pyspark/supermaket.csv'
sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
gender = sdfData.groupBy('Gender').count()
print(gender.show())