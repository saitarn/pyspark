from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("lesson4 spark sql") \
        .getOrCreate()

data_file = '/Users/macbookpro/Desktop/learning/python/pyspark/supermaket.csv'
sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()

sdfData.registerTempTable("sales")

output = scSpark.sql('SELECT * from sales')
output.show()

output = scSpark.sql('SELECT * from sales WHERE `Unit Price` < 15 AND Quantity < 10')
output.show()

output = scSpark.sql('SELECT COUNT(*) as total, City from sales GROUP BY City')
output.show()

output.write.format('json').save('filtered.json')