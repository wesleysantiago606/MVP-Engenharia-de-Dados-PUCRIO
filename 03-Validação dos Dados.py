# Databricks notebook source
# MAGIC %md
# MAGIC Chamada da Casca
# MAGIC

# COMMAND ----------

# MAGIC %run "./00-Casca"

# COMMAND ----------

# MAGIC %md
# MAGIC Validação dos dados pós tratamento
# MAGIC
# MAGIC 1- Verificação do tratamento de outliers
# MAGIC
# MAGIC 2- Verificação dos nulos
# MAGIC
# MAGIC 3- Verificação das categorias das variáveis não numéricas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Carga da base limpa
# MAGIC

# COMMAND ----------

df_clean = spark.table(base_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC Exploração das métricas estatísticas de qualidade
# MAGIC

# COMMAND ----------

display(df_clean.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Verificação de nulos

# COMMAND ----------

var_nulas = df_clean.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) 
    for c in df_clean.columns
])
display(var_nulas)

# COMMAND ----------

# MAGIC %md
# MAGIC Verificar colunas categóricas

# COMMAND ----------

for c in var_cat:
  print(f"Valores distintos de: " + c)
  display(df_clean.select(c).distinct()) 