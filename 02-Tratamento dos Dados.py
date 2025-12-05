# Databricks notebook source
# MAGIC %md
# MAGIC Chamada da casca
# MAGIC

# COMMAND ----------

# MAGIC %run "./00-Casca"

# COMMAND ----------

# MAGIC %md
# MAGIC Carga de base bruta

# COMMAND ----------

df = spark.table(base_credito)


# COMMAND ----------

# MAGIC %md
# MAGIC Cap de idade em person_age

# COMMAND ----------

df = df.withColumn("person_age", F.when(F.col("person_age") > 100, 100).otherwise(F.col("person_age")))

# COMMAND ----------

# MAGIC %md
# MAGIC Cap de tempo de trabalho em person_emp_length

# COMMAND ----------

df = df.withColumn("person_emp_length", F.when(F.col("person_emp_length") > 50, 50).otherwise(F.col("person_emp_length")))

# COMMAND ----------

# MAGIC %md
# MAGIC Completar valores nulos das colunas:
# MAGIC
# MAGIC 1- person_emp_length: mediana dos dados, dessa forma podemos trazer, após o tratamento dos outliers extremos, a mediana é mais robusta pois é menos sensível aos outliers definidos na exploração dos dados como acima de 14,5. Desta forma trazemos como valor de escape para nulos, um valor que representa um comportamento mais alinhado com os valores entre Q1 e Q3 da métrica, evitando assim que a predição da PD, dentro do contexto de risco de crédito, fique subestimada.
# MAGIC
# MAGIC 2- loan_int_rate: Variável com melhor distribuição de dados, portanto, a média da taxa de juros é um bom candidato a valor de escape para valores nulos nesta coluna. Lembrando de fazer esta média agrupada pela variável loan_grade (rating da operação)
# MAGIC

# COMMAND ----------

mediana_emp = df.approxQuantile("person_emp_length", [0.5], 0.01)[0]

df = df.withColumn("person_emp_length", F.when(F.col("person_emp_length").isNull(), mediana_emp)
                   .otherwise(F.col("person_emp_length"))
                   )


# COMMAND ----------

taxa_rating = (df.groupBy("loan_grade")
                     .agg(F.mean("loan_int_rate").alias("media_taxa")))
df = df.join(taxa_rating, on="loan_grade", how="left")

df = df.withColumn("loan_int_rate", F.when(F.col("loan_int_rate").isNull(), F.col("media_taxa"))
                   .otherwise(F.col("loan_int_rate"))
                   ).drop("media_taxa")
                     

# COMMAND ----------

# MAGIC %md
# MAGIC Há um tratamento pertinente na variável cb_person_default_on_file, cujo os dados são "Y" ou "N", que serão convertidos para valores binários de 0 ou 1
# MAGIC

# COMMAND ----------

df = df.withColumn("cb_person_default_on_file", F.when(F.col("cb_person_default_on_file") == "N", 0)
                   .otherwise(1))

# COMMAND ----------

# MAGIC %md
# MAGIC Criar a tabela tratada
# MAGIC

# COMMAND ----------

df.write.format("Delta").mode("overwrite").saveAsTable(base_clean)