# Customer Data Analysis Queries

## 🔹 Basic Queries

1️⃣ **Retrieve all records where SEX is 1 (Male).**

2️⃣ **Get all records where EDUCATION is 2 (University).**

3️⃣ **Find customers older than 40 years.**

4️⃣ **List all records where the LIMIT_BAL is greater than 500,000.**

5️⃣ **Retrieve records where MARRIAGE is 1 (Married).**

---

## 🔹 Aggregation Queries

6️⃣ **Find the average LIMIT_BAL for all customers.**

7️⃣ **Get the total sum of BILL_AMT1 for all customers.**

8️⃣ **Calculate the minimum and maximum values of PAY_AMT3.**

9️⃣ **Count the number of customers with AGE greater than 30.**

🔟 **Find the total number of customers in the dataset.**

---

## 🔹 Grouping Queries

1️⃣1️⃣ **Get the average LIMIT_BAL grouped by SEX.**

1️⃣2️⃣ **Find the total PAY_AMT1 grouped by EDUCATION.**

1️⃣3️⃣ **Calculate the average BILL_AMT6 grouped by MARRIAGE status.**

1️⃣4️⃣ **Find the total number of customers per AGE group** (e.g., 20-30, 31-40, etc.).

1️⃣5️⃣ **Get the average next_month_payment grouped by SEX.**

---

## 🔹 Conditional Queries

1️⃣6️⃣ **Find all customers who had a PAY_0 value greater than 2.**

1️⃣7️⃣ **Retrieve records where PAY_AMT1 is greater than BILL_AMT1.**

1️⃣8️⃣ **Get all records where PAY_2, PAY_3, and PAY_4 are all greater than 0.**

1️⃣9️⃣ **Find customers who have never delayed payments** (PAY_0, PAY_2, …, PAY_6 are all 0 or negative).

2️⃣0️⃣ **List customers who made a payment (PAY_AMT6 > 0) but had a negative BILL_AMT6.**

---

## 🔹 Advanced Queries

2️⃣1️⃣ **Find customers whose total payments (PAY_AMT1 to PAY_AMT6) exceed their total bill amount (BILL_AMT1 to BILL_AMT6).**

2️⃣2️⃣ **Rank customers by LIMIT_BAL in descending order.**

2️⃣3️⃣ **Identify the top 5 customers with the highest next_month_payment.**

2️⃣4️⃣ **Find customers who have consistently increasing bill amounts** (BILL_AMT1 < BILL_AMT2 < ... < BILL_AMT6).

2️⃣5️⃣ **Get the percentage of customers who have PAY_0 greater than 2.**

---
