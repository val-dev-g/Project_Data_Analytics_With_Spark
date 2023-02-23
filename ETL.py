# Databricks notebook source
# MAGIC %sh
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

# MAGIC %python
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC  
# MAGIC spark = (SparkSession
# MAGIC              .builder
# MAGIC              .appName("ETL")
# MAGIC              .enableHiveSupport()
# MAGIC              .getOrCreate()
# MAGIC             )
# MAGIC spark

# COMMAND ----------

# MAGIC %python
# MAGIC  
# MAGIC """orders.csv - Contains the list of orders. The various attributes present in the dataset are
# MAGIC Order ID : The unique identifier for the each order (INTEGER)
# MAGIC Order Date: The date and time when order was placed (DATETIME)
# MAGIC Customer ID : The customer Id of the customer associated to the order (INTEGER)
# MAGIC Order Status : The status associated with the order (VARCHAR)
# MAGIC """
# MAGIC ordersSchema  = StructType([
# MAGIC     StructField("OrderID",IntegerType(),True),  
# MAGIC     StructField("OrderDate",DateType(),True),  
# MAGIC     StructField("CustomerID", IntegerType(), True), 
# MAGIC     StructField("OrderStatus", StringType(), True)   
# MAGIC   ])
# MAGIC  
# MAGIC 
# MAGIC ordersDFWithSchema = (sqlContext.read.format("csv")
# MAGIC    .option("delimiter",",").option("quote","")
# MAGIC   .option("header", "false")
# MAGIC   .schema(ordersSchema)
# MAGIC   .load("dbfs:/datasets/orders.csv"))
# MAGIC  
# MAGIC  
# MAGIC ordersDFWithSchema.show()

# COMMAND ----------

# MAGIC %python
# MAGIC """ 
# MAGIC products.csv - Contains the details about the each product
# MAGIC Product ID: The unique identifier for the each product (INTEGER)
# MAGIC Product Category ID : The identifier for the category to which product belongs (INTEGER)
# MAGIC Product Description : The description associated with the product (VARCHAR)
# MAGIC Product Price : The price of the product (FLOAT)
# MAGIC Product Image : The url of the image associated with the product (VARCHAR)"""
# MAGIC productsSchema  = StructType([
# MAGIC     StructField("ProductID",IntegerType(),True), 
# MAGIC     StructField("CategoryID",IntegerType(),True), 
# MAGIC     StructField("Descriptione",StringType(),True),  
# MAGIC     StructField("Price", DoubleType(), True), 
# MAGIC     StructField("Image", StringType(), True)   
# MAGIC   ])
# MAGIC 
# MAGIC productsDF =(spark.read
# MAGIC                       .option("inferSchema", "true")
# MAGIC                       .option("header", "false")
# MAGIC                       .schema(productsSchema)
# MAGIC                       .csv("dbfs:/datasets/products.csv"))
# MAGIC  
# MAGIC 
# MAGIC display(productsDF)

# COMMAND ----------

# MAGIC %python
# MAGIC  
# MAGIC """categories.csv - Contains the details about the each product
# MAGIC Category ID: The unique identifier for the each category (INTEGER)
# MAGIC Category Department ID : The identifier for the department to which category belongs (INTEGER)
# MAGIC Category Name : The name of the category (VARCHAR)
# MAGIC 
# MAGIC  """
# MAGIC  
# MAGIC categoriesSchema  = StructType([
# MAGIC     StructField("CategoryID",IntegerType(),True), 
# MAGIC     StructField("DepartmentID",IntegerType(),True), 
# MAGIC     StructField("Name",StringType(),True)  
# MAGIC   ])
# MAGIC  
# MAGIC categoriesDF =(spark.read.option("inferSchema", "false").option("header", "false").schema(categoriesSchema)
# MAGIC                        .csv("dbfs:/datasets/categories.csv"))
# MAGIC 
# MAGIC  
# MAGIC categoriesDF.show(10)

# COMMAND ----------

# MAGIC %python
# MAGIC AdventureWorks_Products =(spark.read
# MAGIC                       .option("inferSchema", "true")
# MAGIC                       .option("header", "true")
# MAGIC                       .csv("dbfs:/datasets/AdventureWorks_Products.csv"))
# MAGIC AdventureWorks_Products.show(10)

# COMMAND ----------

# MAGIC %python
# MAGIC  
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
# MAGIC joined_orders_ordersItem = ordersDFWithSchema.join(ordersItemDFWithSchema, ordersItemDFWithSchema.OrderID ==ordersDFWithSchema.OrderID, "inner")
# MAGIC joined_orders_ordersItem = joined_orders_ordersItem.drop(ordersItemDFWithSchema.OrderID)
# MAGIC 
# MAGIC display(joined_orders_ordersItem)

# COMMAND ----------

# MAGIC %python
# MAGIC joined_products_categories = productsDF.join(categoriesDF, categoriesDF.CategoryID == productsDF.CategoryID, "inner")
# MAGIC joined_products_categories = joined_products_categories.drop(productsDF.CategoryID)
# MAGIC display(joined_products_categories)

# COMMAND ----------

# MAGIC %python
# MAGIC joined_all = joined_orders_ordersItem.join(joined_products_categories, joined_products_categories.ProductID == joined_orders_ordersItem.ProductID, "inner")
# MAGIC joined_all = joined_all.drop(joined_products_categories.ProductID)
# MAGIC display(joined_all)

# COMMAND ----------

# MAGIC %python
# MAGIC CustomerSummary = joined_all.select(col("OrderID"),col("CustomerID"),col("OrderDate"),col("Subtotal"),col("ProductPrice")).groupBy("CustomerID").agg(count("OrderID").alias("TotalOrders"),sum("Subtotal").alias("TotalRevenue"),avg("ProductPrice").alias("AvgOrderValue"),max("OrderDate").alias("LastOrderDate"))
# MAGIC display(CustomerSummary)

# COMMAND ----------

# MAGIC %python
# MAGIC ProductSummary = joined_all.select(col("OrderID"),col("CustomerID"),col("Quantity"),col("OrderDate"),col("Subtotal"),col("ProductPrice"),col("CategoryID"),col("Name"),col("ProductID"),col("Price")).groupBy("ProductID","CategoryID","Name").agg(count("OrderID").alias("TotalOrders"),sum("Quantity").alias("TotalQuantity"),sum("Subtotal").alias("TotalRevenue"),avg("Quantity").alias("AvgQuantityOrder"),avg("Price").alias("AvgPrice"))
# MAGIC display(ProductSummary)

# COMMAND ----------

# MAGIC %python
# MAGIC joined_all=joined_all.withColumn("Year", split(col("OrderDate"), "-").getItem(0)).withColumn("Month", split(col("OrderDate"), "-").getItem(1)) 
# MAGIC display(joined_all)

# COMMAND ----------

# MAGIC %python
# MAGIC # Register the DataFrame as a SQL Global temporary view
# MAGIC try:
# MAGIC   CustomerSummary.createGlobalTempView("customer") 
# MAGIC   ProductSummary.createGlobalTempView("product")
# MAGIC   joined_all.createGlobalTempView("joined_all")
# MAGIC except:
# MAGIC   
# MAGIC   print("remove and recreate ")
# MAGIC   #charger un nouveau data frame a partir de resulta de RevenuPrevouisMonthG qui est stocké sous HDFS(Parquet) ou Table Hive
# MAGIC   spark.catalog.dropGlobalTempView("customer") 
# MAGIC   spark.catalog.dropGlobalTempView("product")
# MAGIC   spark.catalog.dropGlobalTempView("joined_all")
# MAGIC   CustomerSummary.createGlobalTempView("customer") 
# MAGIC   ProductSummary.createGlobalTempView("product")
# MAGIC   joined_all.createGlobalTempView("joined_all")
