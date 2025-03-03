from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('CreditCardData').getOrCreate()

data = spark.read.csv('CreditCardDefault.csv', header=True, inferSchema=True)

# Create temp view to run spark sql queries
data.createOrReplaceTempView('customers')

print('1️⃣ Retrieve all records where SEX is 1 (Male).')
gender_df = data.filter(data.SEX==1)
gender_df.show()

print('2️⃣ Get all records where EDUCATION is 2 (University).')
edu_df = data.filter(data.EDUCATION==2)
edu_df.show()

print('3️⃣ Find customers older than 40 years.')
cust_df = data.filter(data.AGE>40)
cust_df.show()

print('4️⃣ List all records where the LIMIT_BAL is greater than 500,000.')
limit_bal_df = data.where(data.LIMIT_BAL > 500000)
limit_bal_df.show()

print('5️⃣ Retrieve records where MARRIAGE is 1 (Married).')
marriage_df = data.where(data.MARRIAGE == 1)
marriage_df.show()

print('6️⃣ Find the average LIMIT_BAL for all customers.')
avg_lb_df = data.agg(avg(data.LIMIT_BAL).alias('Avg of LIMIT_BAL'))
avg_lb_df.show()

print('7️⃣ Get the total sum of BILL_AMT1 for all customers.')
total_bill_amt1_df = data.agg(sum(data.BILL_AMT1).alias('Total Of BILL_AMT1'))
total_bill_amt1_df.show()

print('8️⃣ Calculate the minimum and maximum values of PAY_AMT3.')
pay_amt3_min_df = data.agg(min(data.PAY_AMT3).alias('MIN of PAY_AMT3'))
pay_amt3_min_df.show()

pay_amt3_max_df = data.agg(max(data.PAY_AMT3).alias('MAX of PAY_AMT3'))
pay_amt3_max_df.show()

print('9️⃣ Count the number of customers with AGE greater than 30.')
age30_df = data.where(data.AGE > 30).agg(count(data.AGE).alias('AGE > 30'))
age30_df.show()
# spark.sql('select count(1) from customers where AGE > 30').show()
# spark.sql('select count(1) from customers where AGE < 30').show()
# spark.sql('select count(1) from customers where AGE = 0 ').show()

age30_df = data.where(data.AGE < 30).agg(count(data.AGE).alias('AGE < 30'))
age30_df.show()

print('🔟 Find the total number of customers in the dataset.')
total_cust_df = data.agg(count(data.ID))
total_cust_df.show()

print('1️⃣1️⃣ Get the average LIMIT_BAL grouped by SEX.')
avg_bal = data.groupBy(data.SEX).agg(avg(data.LIMIT_BAL).alias('AVG limit bal'))
avg_bal.show()