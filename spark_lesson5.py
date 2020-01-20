from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("lesson5 loadtojson") \
        .getOrCreate()

data_file = '/Users/macbookpro/Desktop/learning/python/pyspark/supermaket.csv'
sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()

sdfData.registerTempTable("sales")

output = scSpark.sql('SELECT COUNT(*) as total, City from sales GROUP BY City')
output.show()

output.coalesce(1).write.mode('overwrite').format('json').save('filtered.json')
# output.coalesce(1).write.format('json').save('filtered.json')
# output.write.format('json').save('filtered.json')