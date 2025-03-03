from typing import final

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import month

spark = SparkSession.builder.appName('Ecommerce').getOrCreate()

customer_df = spark.read.csv('customers.csv').toDF('customer_id', 'customer_name');
customer_df.createOrReplaceTempView('customers')

products_df = spark.read.csv('products.csv').toDF('product_id','product_name','price')
products_df.createOrReplaceTempView('products')

orders_df = spark.read.csv('orders.csv').toDF('order_id','customer_id','product_id','amount','order_date')
orders_df.createOrReplaceTempView('orders')

print('++++++++++++++++++ Display data sets ++++++++++++++++++')
customer_df.show()
products_df.show()
orders_df.show()

print('1️⃣ Get all orders placed by a specific customer (e.g., ‘Alice’ id = 1).')
alice_df = orders_df.filter('customer_id = 1')
result_df = alice_df.join(customer_df, 'customer_id', 'inner')
result_df.show()

print('2️⃣ Retrieve all orders for a specific product (e.g., ‘Laptop’ id = 101).')
prod_df = orders_df.filter('product_id = 101')
result = prod_df.join(products_df, 'product_id', 'inner')
result.show()

print('3️⃣ Find the total number of orders placed so far.')
# print(orders_df.count())
orders_df.select(F.count('*').alias('total_order')).show()
# orders_df.selectExpr('count(*) as total_orders').show()

print('4️⃣ Find the total revenue generated from all orders.')
# Query : display product_id, revenue, product_name
result = orders_df.groupBy('product_id').agg(F.sum('amount').alias('total_revenue')).join(products_df, 'product_id',
                                                                                          'left')
result.show()

print('5️⃣ List all customers and their total spending (order total).')
cust_spend = orders_df.groupBy('customer_id').agg(F.sum('amount'));
cust_spend.show()

result = customer_df.join(cust_spend, 'customer_id', 'left')
result.show()

print('6️⃣ Find the top 5 highest-selling products based on total revenue.')
product_revenue = orders_df.groupBy('product_id').agg(F.sum('amount').alias('total')).sort(F.desc('total')).limit(5)
product_revenue.show()

print('7️⃣ Get a monthly sales trend report (total sales per month).')
# select MONTH(order_date), sum(amount) as total from orders group by MONTH(order_date) order by total desc;
monthly_df = (orders_df.withColumn('month', F.expr('MONTH(order_date)'))
              .groupBy('month')).agg(F.sum('amount').alias('total'))
monthly_df.show()

print('8️⃣ Find the customer who has spent the most money.')
# i want to show only customer name column from the customer df
cust_spend = (orders_df.groupBy('customer_id')
              .agg(F.sum('amount').alias('total_spend'))
              .sort(F.desc('total_spend')).limit(1)).join(customer_df, 'customer_id', 'left')
cust_spend.show()

print('9️⃣ Identify products that have never been ordered.')
# How to identify which data sets shoudle be inthe left and right
prod_df = products_df.join(orders_df, 'product_id', 'anti')
prod_df.show()

print('🔟 Find the day with the highest total sales.')
# spark.sql('select DAY(order_date), sum(amount) as total_sales from orders group by DAY(order_date) order by total_sales desc limit 1').show()
high_df = (
    orders_df.withColumn('day', F.expr('DAY(order_date)'))
    .groupBy('day')
    .agg(F.sum('amount').alias('total_sales'))
    .sort(F.desc('total_sales'))
    .limit(1)
)
high_df.show()

print('1️⃣1️⃣ Find the top 3 customers who have placed the most orders.')
# spark.sql(
#     'select customer_id, count(1) as total_orders from orders group by customer_id order by total_orders desc limit 3;').show()

result = (orders_df.groupBy('customer_id')
          .agg(F.count('customer_id').alias('total_orders'))
          .sort(F.desc('total_orders'))
          .limit(3).join(customer_df, 'customer_id', 'inner'))
result.show()

print('1️⃣2️⃣ Identify the most popular product category (if a category column exists).')
print('No category in the database')

print('1️⃣3️⃣ Find the average order value per customer.')
# spark.sql(
#     'select customer_id, sum(amount) as total_spent, count(customer_id) as total_orders, (sum(amount)/count(customer_id)) as avg from orders group by customer_id').show()

avg_df = (orders_df.groupBy('customer_id')
          .agg(F.sum('amount').alias('total_spent'), F.count('customer_id').alias('total_orders'))
          .withColumn('avg', F.expr('total_spent/total_orders')))

avg_df.show()

print('1️⃣4️⃣ Find the customer who placed the earliest order.')
# spark.sql('select a.*, b.customer_name from orders a join customers b on a.customer_id = b.customer_id order by order_date asc limit 1').show()
# spark.sql('select a.customer_name,a.customer_id, o.order_date from customers a join orders o where o.order_date = (select min(order_date) from orders) limit 1').show()
result = orders_df.withColumn('dense_rank', F.expr('dense_rank() over (partition by order_date order by order_date)'))
finalresult = result.filter('dense_rank = 1').limit(1).drop('dense_rank')
finalresult.show()

print('1️⃣5️⃣ Get the total sales for the last 6 months.')
# spark.sql('select sum(amount) as 6_month_sales from orders where order_date between DATE_SUB(CURRENT_DATE, 180) AND CURRENT_DATE').show()
orders_df.filter('order_date between date_sub(current_date, 180) and current_date').agg(
    F.sum('amount').alias('6 months total sales')).show()

print('1️⃣6️⃣ Identify customers who have not placed any orders in the last 3 months.')
# spark.sql('select c.customer_id, c.customer_name from customers c where not exists (select 1 from orders o where o.customer_id = c.customer_id)').show()
orders = orders_df.withColumn('order_date', F.expr('cast(order_date as DATE)'))
last3month = F.expr('date_sub(current_date(), 90)')
recentOrders = orders.filter(orders.order_date >= last3month)
customer_df.join(recentOrders, 'customer_id', 'left_anti').show()

print('1️⃣6️⃣ Identify Products That Have Never Been Sold')
# spark.sql('select p.product_id, p.product_name from products p where not exists (select 1 from orders o where o.product_id = p.product_id)').show()
products_df.join(orders_df, 'product_id', 'leftanti').show()

print('1️⃣6️⃣ Identify Products That Have Been Sold In Last 30 Days')
orders = orders_df.withColumn('order_date', F.expr('cast(order_date as Date)'))
months = F.date_sub(F.current_date(), 30)
orders.filter(
    (orders.order_date >= months) & (orders.order_date < F.current_date())
).sort(F.desc(orders.order_date)).show()

print('1️⃣6️⃣ Identify Products That Have Never Been Sold In Last 30 Days')
recent_orders = orders.filter(
    (orders.order_date >= F.date_sub(F.current_date(), 30))
    & (orders.order_date < F.current_date())).sort(
    F.desc('order_date'))

products_df.join(recent_orders, 'product_id', 'left_anti').show()


print('1️⃣7️⃣ Find the total revenue generated in each quarter of the year.')
# spark.sql('select sum(amount) as total_sales, quarter(order_date) as quarter from orders group by quarter(order_date)')
orders_df = orders_df.withColumn('order_date', F.expr('cast(order_date as date)'))
orders_df.groupBy(F.quarter(orders_df.order_date).alias('Quarter')).agg(F.sum(orders_df.amount).alias('total_revenue')).show()

# print('1️⃣8️⃣ Identify the product with the highest and lowest revenue.')
# result = (orders_df.groupBy('product_id')
#           .agg(F.sum(orders_df.amount).alias('total_revenue'))
#           .agg(F.count('product_id').alias('total'))
#           )
# result.show()

# orders_df.groupBy('product_id').agg(F.count('product_id').alias('total')).agg(F.min('total')).show()