# pyspark

SparkSession เป็น จุดเริ่มจ้นของการใช้งาน PySpark application ทำให้เราสามารถใช้งาน DataSet และ DataFrame ได้
DataSet และ DataFrame เป็น APIs ที่ Spark สร้างมาให้เราใช้งาน

import Spark Session โดยเริ่มต้นต้อง import ตามนี้

from pyspark.sql import SparkSession

from pyspark.sql import SQLContext

หลังจากนั้น เราต้องสร้าง Spark Application ของเราด้วยคำสั่ง
scSpark = SparkSession.builder.appName("reading csv").getOrCreate()

ซึ่งเป็นการเรียกใช้ Class SparkSession ที่ได้ import มาตอนต้น เพื่อสร้าง  appName ในที่นี้เราตั้งชื่อว่า "reading csv"
โดยมี method เป็นตัว return ค่า SparkSessin ที่เราได้สร้างขึ้นมา และเก็บไว้ที่ตัวแปร scSpark



