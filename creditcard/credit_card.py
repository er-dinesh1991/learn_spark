from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName('CreditCardData').getOrCreate()

data = spark.read.csv('CreditCardDefault.csv', header=True, inferSchema=True)
# data.show(1)
# data.printSchema()

# edu_df = data.filter('EDUCATION = 2').agg(count('EDUCATION').alias('EDUCATION is 2'))
# edu_df.show()


# data.filter('AGE > 40').agg(count('*').alias('AGE>40')).show()

