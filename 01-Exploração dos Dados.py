# Databricks notebook source
# MAGIC %md
# MAGIC Chamada da Casca

# COMMAND ----------

# MAGIC %run "./00-Casca"

# COMMAND ----------

#Carga da tabela
df = spark.table(base_credito)
display(df.show(5))

# COMMAND ----------


#Mostrar os tipos de colunas
df.printSchema()

# COMMAND ----------

#Contagem de linhas
df.count()

# COMMAND ----------

#Validar a qualidade inicial dos dados
display(df.describe())


# COMMAND ----------

# MAGIC %md
# MAGIC Encontrando valores nulos na base para mapear tratamentos
# MAGIC

# COMMAND ----------

#Verificar a qualidade dos dados como nulos e vazios
df_nulls = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])
df_nulls.show()

#person_emp_length contém valores nulos ou vazios
#loan_int_rate contém valores nulos ou vazios


# COMMAND ----------

# MAGIC %md
# MAGIC Buscar outliers extremos na base que impactem o tratamento dos nulos catalogando em uma tabela 

# COMMAND ----------

colunas_numericas = [c for c in df.columns
                     if isinstance(df.schema[c].dataType, NumericType)]
colunas_numericas

# COMMAND ----------

# MAGIC %md
# MAGIC Criando tabela com as variáveis estatísticas de Q1, Q3, IQR, limite inferior e superior para análise das variáveis numéricas e encontrar outliers extremos para tratamento.
# MAGIC

# COMMAND ----------

outliers_limite = []

for c in colunas_numericas:
  q1, q3 = df.approxQuantile(c, [0.25, 0.75], 0.01)
  iqr = q3 - q1

  lower = q1 - 1.5 * iqr
  upper = q3 + 1.5 * iqr

  p99 = df.approxQuantile(c, [0.99], 0.01)[0]

  outliers_limite.append((c, q1, q3, iqr, lower, upper, p99))

df_base_outliers = spark.createDataFrame(outliers_limite, ["coluna", "q1", "q3", "iqr", "limite_inferior", "limite_superior", "p99"])
display(base_outliers)

df_base_outliers.write.format("Delta").mode("overwrite").saveAsTable(base_outliers)

# COMMAND ----------

# MAGIC %md
# MAGIC Decisões sobre os outliers
# MAGIC
# MAGIC 1- person_age: Limites estatísticos superior e inferior não atendem a regra de negócio, fazer o cap de outlier extremo em 100 anos, o valor de 144 é irreal para a idade de uma pessoa, indica inconsistência da base de dados
# MAGIC
# MAGIC 2- person_income: Percentil 99 em US$ 6.000.000,00, indica renda muito alta, mas não demonstra erro ou inconsistência da base, não atuar sobre esta coluna
# MAGIC
# MAGIC 3- person_emp_length: Percentil 99 de 123 anos, valor irreal para tempo de emprego, fazer cap de outlier extremo para tempo de trabalho em 50 anos
# MAGIC
# MAGIC 4- loan_amnt: Coluna com valores consistentes, não atuar
# MAGIC
# MAGIC 5- loan_int_rate: Coluna com valores consistentes, não atuar
# MAGIC
# MAGIC 6- loan_status: Coluna com valores binários, não atuar
# MAGIC
# MAGIC 7- loan_percent_income: Coluna com valores consistentes, não atuar
# MAGIC
# MAGIC 8- cb_person_hist_length: Coluna com valores consistentes, não atuar