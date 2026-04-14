# Databricks notebook source
"""Exploration notebook – ad-hoc queries against pipeline output tables.

Use this notebook for interactive investigation of tables produced by a
Lakeflow Spark Declarative Pipeline.  It is intentionally kept separate
from the pipeline source files so that exploratory code never ends up in
production pipeline libraries.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect available schemas

# COMMAND ----------

# List all schemas in the target catalog
display(spark.sql("SHOW SCHEMAS IN test_catalog"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Preview bronze tables

# COMMAND ----------

display(spark.table("test_catalog.test_bronze_schema.fake_orders").limit(20))

# COMMAND ----------

display(spark.table("test_catalog.test_bronze_schema.fake_products").limit(20))

# COMMAND ----------

display(spark.table("test_catalog.test_bronze_schema.fake_users").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Row counts across layers

# COMMAND ----------

# Quick row-count summary for all pipeline output tables
tables = [
    "test_catalog.test_bronze_schema.fake_orders",
    "test_catalog.test_bronze_schema.fake_products",
    "test_catalog.test_bronze_schema.fake_users",
]

for tbl in tables:
    count = spark.table(tbl).count()
    print(f"{tbl}: {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema inspection

# COMMAND ----------

spark.table("test_catalog.test_bronze_schema.fake_orders").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ad-hoc SQL exploration

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT
            o.userid,
            u.name,
            COUNT(*)        AS order_count,
            SUM(o.amount)   AS total_spent
        FROM test_catalog.test_bronze_schema.fake_orders AS o
        JOIN test_catalog.test_bronze_schema.fake_users  AS u
          ON o.userid = u.userid
        GROUP BY o.userid, u.name
        ORDER BY total_spent DESC
        LIMIT 10
        """
    )
)
