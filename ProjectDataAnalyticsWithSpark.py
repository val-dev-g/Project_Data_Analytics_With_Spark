# Databricks notebook source
# MAGIC %md
# MAGIC Telechargement des ensemble des données et enregistrement dans le tmp

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp
# MAGIC wget -P /tmp "https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/orders.csv"
# MAGIC wget -P /tmp "https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/order_items.csv"
# MAGIC wget -P /tmp "https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/products.csv"
# MAGIC wget -P /tmp "https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/retail_db/categories.csv"
# MAGIC wget -P /tmp "https://raw.githubusercontent.com/msellamiTN/Apache-Spark-Training/master/Adventure%20Works/AdventureWorks_Products.csv"

# COMMAND ----------

# MAGIC %python
# MAGIC localOrderFilePath = "file:/tmp/orders.csv"
# MAGIC localOrderItemFilePath = "file:/tmp/order_items.csv"
# MAGIC localProductFilePath = "file:/tmp/products.csv"
# MAGIC localCategoriesFilePath = "file:/tmp/categories.csv"
# MAGIC localAdventureWorks_ProductsPath = "file:/tmp/AdventureWorks_Products.csv"
# MAGIC dbutils.fs.mkdirs("dbfs:/datasets")
# MAGIC dbutils.fs.cp(localOrderFilePath, "dbfs:/datasets/")#hadoop fs -put * /hdfs
# MAGIC dbutils.fs.cp(localOrderItemFilePath, "dbfs:/datasets")
# MAGIC dbutils.fs.cp(localProductFilePath, "dbfs:/datasets/")
# MAGIC dbutils.fs.cp(localCategoriesFilePath, "dbfs:/datasets")
# MAGIC dbutils.fs.cp(localAdventureWorks_ProductsPath, "dbfs:/datasets")
# MAGIC display(dbutils.fs.ls("dbfs:/datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC Chargement des bibliothèques, creation de sparksession et Construction des DataFrames

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC  
# MAGIC spark = (SparkSession
# MAGIC              .builder
# MAGIC              .appName("Retail Data Analysis With SPARK DataFrame")
# MAGIC              .enableHiveSupport()
# MAGIC              .getOrCreate()
# MAGIC             )
# MAGIC ordersDF = spark.read.csv("dbfs:/datasets/orders.csv")
# MAGIC order_itemsDF = spark.read.csv("dbfs:/datasets/order_items.csv")
# MAGIC productsDF = spark.read.csv("dbfs:/datasets/products.csv")
# MAGIC categoriesDF = spark.read.csv("dbfs:/datasets/categories.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Schéma

# COMMAND ----------

# MAGIC %python
# MAGIC  
# MAGIC  
# MAGIC ordersSchema  = StructType([
# MAGIC     StructField("OrderID",IntegerType(),True),  
# MAGIC     StructField("OrderDate",DateType(),True),  
# MAGIC     StructField("CustomerID", IntegerType(), True), 
# MAGIC     StructField("OrderStatus", StringType(), True)   
# MAGIC   ])
# MAGIC  
# MAGIC ordersDFWithSchema = (sqlContext.read.format("csv")
# MAGIC    .option("delimiter",",").option("quote","")
# MAGIC   .option("header", "false")
# MAGIC   .schema(ordersSchema)
# MAGIC   .load("dbfs:/datasets/orders.csv"))
# MAGIC 
# MAGIC ordersDFWithSchema.show()

# COMMAND ----------

# MAGIC %python
# MAGIC ordersItemSchema =( StructType([
# MAGIC      StructField("OrderItemID",IntegerType()),
# MAGIC      StructField("OrderID",IntegerType()),
# MAGIC      StructField("ProductID",IntegerType()),
# MAGIC      StructField("Quantity",DoubleType()),
# MAGIC      StructField("Subtotal",DoubleType()),
# MAGIC      StructField("ProductPrice",DoubleType())]
# MAGIC      )
# MAGIC                   )
# MAGIC  
# MAGIC ordersItemDFWithSchema =(sqlContext
# MAGIC                           .read
# MAGIC                           .format("csv")
# MAGIC                           .option("header", "false")
# MAGIC                           .schema(ordersItemSchema)
# MAGIC                           .csv("dbfs:/datasets/order_items.csv")
# MAGIC                          )
# MAGIC ordersItemDFWithSchema.show(5)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC productsSchema  = StructType([
# MAGIC     StructField("ProductID",IntegerType(),True), 
# MAGIC     StructField("CategoryID",IntegerType(),True), 
# MAGIC     StructField("Descriptione",StringType(),True),  
# MAGIC     StructField("Price", DoubleType(), True), 
# MAGIC     StructField("Image", StringType(), True)   
# MAGIC   ])
# MAGIC  
# MAGIC productsDFWithSchema =(spark.read
# MAGIC                       .option("inferSchema", "true")
# MAGIC                       .option("header", "false")
# MAGIC                       .schema(productsSchema)
# MAGIC                       .csv("dbfs:/datasets/products.csv"))
# MAGIC  
# MAGIC  
# MAGIC productsDFWithSchema.show()

# COMMAND ----------

categoriesSchema  = StructType([
    StructField("CategoryID",IntegerType(),True), 
    StructField("DepartmentID",IntegerType(),True), 
    StructField("Name",StringType(),True)  
  ])
 
categoriesDFWithSchema =(spark.read.option("inferSchema", "false").option("header", "false").schema(categoriesSchema)
                       .csv("dbfs:/datasets/categories.csv"))
 
 
categoriesDFWithSchema.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Les DataFrames ciblés

# COMMAND ----------

# MAGIC %python
# MAGIC # Register the DataFrame as a SQL temporary view
# MAGIC ordersDFWithSchema.createOrReplaceTempView("orders")
# MAGIC ordersItemDFWithSchema.createOrReplaceTempView("order_items")
# MAGIC productsDFWithSchema.createOrReplaceTempView("products")
# MAGIC categoriesDFWithSchema.createOrReplaceTempView("categories")

# COMMAND ----------

# MAGIC %md
# MAGIC Total Orders, Total Revenue et Avg Order Value par client

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CustomerID as `CustomerID`,
# MAGIC   COUNT(DISTINCT o.OrderID) as `Total Orders`,
# MAGIC   SUM(Subtotal) as `Total Revenue`,
# MAGIC   AVG(Subtotal) as `Avg Order Value`
# MAGIC FROM orders o
# MAGIC JOIN order_items oi ON o.OrderID = oi.OrderID
# MAGIC GROUP BY CustomerID

# COMMAND ----------

# MAGIC %md
# MAGIC Last Order Date par client

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CustomerID as `CustomerID`,
# MAGIC   MAX(OrderDate) as `Last Order Date`
# MAGIC FROM orders
# MAGIC GROUP BY CustomerID

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame récapitulative du produit :
# MAGIC a) Total Orders, Total Quantity, Total Revenue, Avg Quantity/Order et Avg Price par produit

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   oi.ProductID,
# MAGIC   COUNT(DISTINCT oi.OrderID) as `Total Orders`,
# MAGIC   SUM(Quantity) as `Total Quantity`,
# MAGIC   SUM(Subtotal) as `Total Revenue`,
# MAGIC   AVG(Quantity) as `Avg Quantity/Order`,
# MAGIC   AVG(ProductPrice) as `Avg Price`
# MAGIC FROM orders o , order_items oi
# MAGIC where o.OrderID=oi.OrderID
# MAGIC GROUP BY ProductID

# COMMAND ----------

# MAGIC %md
# MAGIC b) Avg Discount, Category ID et Category Name par produit

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   oi.ProductID as `Product ID`,
# MAGIC   AVG(1 - (Subtotal / (Quantity * ProductPrice))) as `Avg Discount`,
# MAGIC   p.CategoryID as `Category ID`,
# MAGIC   c.Name as `Category Name`
# MAGIC FROM order_items oi
# MAGIC JOIN products p ON oi.ProductID = p.ProductID
# MAGIC JOIN categories c ON p.CategoryID = c.CategoryID
# MAGIC GROUP BY oi.ProductID, p.CategoryID, c.Name

# COMMAND ----------

# MAGIC %python
# MAGIC custumerDF3 = ordersDFWithSchema.groupBy("CustomerID").count()
# MAGIC display(custumerDF3)

# COMMAND ----------

# MAGIC %python
# MAGIC rename=custumerDF3.withColumnRenamed("count","Total Orders")
# MAGIC  
# MAGIC rename.show()
# MAGIC  

# COMMAND ----------

# MAGIC %python
# MAGIC  
# MAGIC joined_Revenue=ordersDFWithSchema.join(ordersItemDFWithSchema, ordersItemDFWithSchema.OrderID ==ordersDFWithSchema.OrderID, "inner")
# MAGIC  
# MAGIC  
# MAGIC joined_Revenue.show()

# COMMAND ----------

# MAGIC %python
# MAGIC joined_Revenue=joined_Revenue.drop(ordersItemDFWithSchema.OrderID)
# MAGIC 
# MAGIC total_orders = joined_Revenue.groupBy("CustomerID").agg(count("OrderID").alias("Total Orders")) 
# MAGIC total_orders.show()
# MAGIC 
# MAGIC total_orders1 = joined_Revenue.join(total_orders, "CustomerID", "left")
# MAGIC total_orders1.show()
# MAGIC total_revenu = joined_Revenue.groupBy("CustomerID").agg(sum("Subtotal").alias("Total Revenue")) 
# MAGIC total_revenu.show()
# MAGIC total_revenue = total_orders.join(total_revenu, "CustomerID", "left")
# MAGIC total_revenue.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 1-Analyser le chiffre d'affaires par mois et par année pour identifier les mois et années les plus rentables :

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT YEAR(o.OrderDate) AS OrderYear, MONTH(o.OrderDate) AS OrderMonth, SUM(oi.Subtotal) AS Revenue 
# MAGIC FROM orders o
# MAGIC JOIN order_items oi ON o.OrderID = oi.OrderID
# MAGIC GROUP BY OrderYear, OrderMonth
# MAGIC ORDER BY OrderYear, OrderMonth 

# COMMAND ----------

revenue = ordersDFWithSchema.join(ordersItemDFWithSchema, ordersDFWithSchema.OrderID == ordersItemDFWithSchema.OrderID) \
    .groupBy(year("OrderDate").alias("year"), month("OrderDate").alias("month")) \
    .agg(round(sum("Subtotal"), 2).alias("revenue")) \
    .orderBy(desc("revenue"))
revenue.show()

# COMMAND ----------

# MAGIC %md
# MAGIC  2-Identifier les clients les plus fidèles en calculant leur fréquence d'achat et leur valeur totale des commandes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o.CustomerID AS CustomerID, COUNT(DISTINCT o.OrderID) AS TotalOrders, SUM(oi.Subtotal) AS TotalRevenue
# MAGIC FROM orders o
# MAGIC JOIN order_items oi ON o.OrderID = oi.OrderID
# MAGIC GROUP BY o.CustomerID
# MAGIC ORDER BY TotalRevenue DESC

# COMMAND ----------

joined_Revenue=ordersDFWithSchema.join(ordersItemDFWithSchema, ordersItemDFWithSchema.OrderID ==ordersDFWithSchema.OrderID, "inner")
joined_Revenue=joined_Revenue.drop(ordersItemDFWithSchema.OrderID)
loyal_customers = joined_Revenue.groupBy("CustomerID").agg(count("OrderID").alias("Total Orders"),round(sum("Subtotal"), 2).alias("total_spent"))
loyal_customers= loyal_customers.orderBy(desc("total_spent"))
loyal_customers.show()




# COMMAND ----------

# MAGIC %md
# MAGIC 3-Identifier les produits les moins vendus pour réduire les coûts d'inventaire

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT oi.ProductID AS ProductID, SUM(oi.Quantity) AS TotalQuantity
# MAGIC FROM order_items oi
# MAGIC GROUP BY oi.ProductID
# MAGIC ORDER BY TotalQuantity ASC

# COMMAND ----------

# MAGIC %md
# MAGIC  4- Analyser le comportement d'achat des clients pour identifier les produits couramment achetés ensemble (analyse de corrélation) 

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH product_pairs AS (
# MAGIC   SELECT oi1.ProductID AS Product1, oi2.ProductID AS Product2, COUNT(DISTINCT oi1.OrderID) AS OrderCount
# MAGIC   FROM order_items oi1
# MAGIC   JOIN order_items oi2 ON oi1.OrderID = oi2.OrderID AND oi1.ProductID < oi2.ProductID
# MAGIC   GROUP BY oi1.ProductID, oi2.ProductID
# MAGIC ), 
# MAGIC ranked_pairs AS (
# MAGIC   SELECT *, RANK() OVER (ORDER BY OrderCount DESC) AS Rank
# MAGIC   FROM product_pairs
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked_pairs
# MAGIC WHERE Rank <= 10

# COMMAND ----------

# MAGIC %md
# MAGIC 5-Identifier les heures de pointe des ventes pour mieux planifier les ressources en personnel 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT HOUR(o.OrderDate) AS OrderHour, COUNT(*) AS TotalOrders
# MAGIC FROM orders o
# MAGIC GROUP BY OrderHour,OrderDate
# MAGIC ORDER BY TotalOrders DESC

# COMMAND ----------

peak_hours = ordersDFWithSchema.groupBy(hour("OrderDate").alias("hour")) \
    .agg(countDistinct("OrderID").alias("order_count")) \
    .orderBy(desc("order_count"))

peak_hours.show()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour

spark = SparkSession.builder.appName("PeakHoursAnalysis").getOrCreate()


# Convert Order Date to timestamp type
ordersDFWithSchema = ordersDFWithSchema.withColumn("OrderDate", ordersDFWithSchema["OrderDate"].cast("timestamp"))

# Extract hour from Order Date
ordersDFWithSchema = ordersDFWithSchema.withColumn("OrderHour", hour("OrderDate"))

# Group by Order Hour and count the number of orders
peak_hours = ordersDFWithSchema.groupBy("OrderHour").count().orderBy("OrderHour")

peak_hours.show()


# COMMAND ----------

# MAGIC %md
# MAGIC 7- Analyser la distribution géographique des ventes pour identifier les régions les plus rentables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   o.OrderStatus,
# MAGIC   COUNT(DISTINCT oi.OrderID) as `Total Orders`,
# MAGIC   SUM(oi.Subtotal) as `Total Revenue`,
# MAGIC   c.Region
# MAGIC FROM orders o, order_items oi,categories c
# MAGIC JOIN order_items oi ON o.OrderID = oi.OrderID
# MAGIC JOIN categories c ON o.CustomerID = c.CustomerID
# MAGIC WHERE o.OrderStatus = 'SHIPPED'
# MAGIC GROUP BY o.OrderStatus, c.Region
# MAGIC ORDER BY `Total Revenue` DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 8- Identifier les commandes qui ont été annulées ou retournées et les raisons de ces 
# MAGIC annulations/retours

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OrderID, OrderStatus FROM orders WHERE OrderStatus = 'CANCELED' OR OrderStatus = 'RETURNED'

# COMMAND ----------

# Filtrer les commandes annulées ou retournées
cancelled_orders = ordersDFWithSchema.filter((ordersDFWithSchema["OrderStatus"] == "CANCELED") | (ordersDFWithSchema["OrderStatus"] == "RETURNED"))

# Afficher les raisons dans une colonne distincte
cancelled_orders = cancelled_orders.withColumn("reason", when(col("OrderStatus") == "CANCELED", col("CustomerID")).otherwise(col("OrderStatus")))

cancelled_orders.show()


# COMMAND ----------

# MAGIC %md
# MAGIC 9- Suivre l'évolution des ventes et identifier les tendances de vente au fil du temps

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   YEAR(OrderDate) as `Year`,
# MAGIC   MONTH(OrderDate) as `Month`,
# MAGIC   SUM(oi.Subtotal) as `Revenue`
# MAGIC FROM orders o
# MAGIC JOIN order_items oi ON o.OrderID = oi.OrderID
# MAGIC GROUP BY  MONTH(OrderDate), YEAR(OrderDate)
# MAGIC ORDER BY  `Month`,`Year`
