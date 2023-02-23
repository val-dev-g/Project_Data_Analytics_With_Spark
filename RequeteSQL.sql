-- Databricks notebook source
-- MAGIC %sql
-- MAGIC select * from global_temp.joined_all

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from global_temp.customer 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from global_temp.product 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Analyser le chiffre d'affaires par mois et par année pour identifier les mois et années les plus 
-- MAGIC rentables

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select year, month, sum(Subtotal) as TotalRevenue from global_temp.joined_all group by month, year

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Identifier les clients les plus fidèles en calculant leur fréquence d'achat et leur valeur totale des 
-- MAGIC commandes :

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select CustomerID, count(OrderID) as TotalOrders, sum(Subtotal) as TotalRevenue from global_temp.joined_all group by CustomerID order by TotalOrders desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Identifier les produits les moins vendus pour réduire les coûts d'inventaire

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select ProductID, TotalRevenue from global_temp.product order by TotalRevenue asc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Analyser le comportement d'achat des clients pour identifier les produits couramment achetés 
-- MAGIC ensemble (analyse de corrélation) :

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC WITH product_pairs AS (
-- MAGIC   SELECT oi1.ProductID AS Product1, oi2.ProductID AS Product2, COUNT(DISTINCT oi1.OrderID) AS OrderCount
-- MAGIC   FROM global_temp.joined_all oi1
-- MAGIC   JOIN global_temp.joined_all oi2 ON oi1.OrderID = oi2.OrderID AND oi1.ProductID < oi2.ProductID
-- MAGIC   GROUP BY oi1.ProductID, oi2.ProductID
-- MAGIC ), 
-- MAGIC ranked_pairs AS (
-- MAGIC   SELECT *, RANK() OVER (ORDER BY OrderCount DESC) AS Rank
-- MAGIC   FROM product_pairs
-- MAGIC )
-- MAGIC SELECT *
-- MAGIC FROM ranked_pairs
-- MAGIC WHERE Rank <= 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select j1.ProductID as Product1_ID, j2.ProductID as Product1_ID,corr(j1.Quantity, j2.Quantity) as correlation from global_temp.joined_all j1 join global_temp.joined_all j2 on j1.OrderID = j2.OrderID and j1.ProductID < j2.ProductID group by j1.ProductID, j2.ProductID having correlation > 0.5 order by correlation desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Identifier les heures de pointe des ventes pour mieux planifier les ressources en personnel :

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC select hour(OrderDate) as hour, count(OrderID) as TotalOrders from global_temp.joined_all group by hour order by TotalOrders desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Analyser la distribution géographique des ventes pour identifier les régions les plus rentables  : Pas fait car absence de la table CUSTOMER 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Identifier les commandes qui ont été annulées ou retournées et les raisons de ces annulations/retours 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select OrderID, Descriptione, Price ,OrderStatus from global_temp.joined_all where OrderStatus like 'CANCELED' or  OrderStatus like  'RETURNED'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Suivre l'évolution des ventes et identifier les tendances de vente au fil du temps 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select OrderDate, sum(Subtotal) as Revenue from global_temp.joined_all group by OrderDate 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT 
-- MAGIC   YEAR(OrderDate) as `Year`,
-- MAGIC   MONTH(OrderDate) as `Month`,
-- MAGIC   SUM(o.Subtotal) as `Revenue`
-- MAGIC FROM global_temp.joined_all o
-- MAGIC GROUP BY  MONTH(OrderDate), YEAR(OrderDate)
-- MAGIC ORDER BY  `Month`,`Year`
