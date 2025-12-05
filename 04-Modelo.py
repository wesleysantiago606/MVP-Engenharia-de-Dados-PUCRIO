# Databricks notebook source
# MAGIC %md
# MAGIC Carga da base tratada na validação

# COMMAND ----------

# MAGIC %run "./00-Casca"

# COMMAND ----------

df_clean = spark.table(base_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC 1- Conversão da base para Pandas
# MAGIC
# MAGIC 2- One-hot encoding para variáveis categóricas
# MAGIC
# MAGIC 3- Separação da base em X e Y (variáveis independentes e variável resposta)
# MAGIC
# MAGIC 4- Split Treino/Teste
# MAGIC
# MAGIC 6- Treino do Modelo

# COMMAND ----------

pdf_clean = df_clean.toPandas()

#One-hot encoding
pdf_clean = pd.get_dummies(
    pdf_clean,
    columns=var_cat,
    drop_first = True
)

X = pdf_clean.drop(columns=[target])
y = pdf_clean[target]

#Split Treino Teste
X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    stratify=y,
    random_state=42
    )

#Define um Scaler para normalizar os dados da base de treino
scaler = StandardScaler()
X_train_scaler = scaler.fit_transform(X_train)
X_test_scaler = scaler.transform(X_test)

#Chamada do modelo de Regressão Logística
modelo_pd = LogisticRegression(max_iter=1000)
modelo_pd.fit(X_train_scaler, y_train)

#Cálculo das Probabilidades de Default (PD)
y_pd = modelo_pd.predict_proba(X_test_scaler)[:,1]





# COMMAND ----------

# MAGIC %md
# MAGIC Bloco de Métricas de análise estatística do desempenho do Modelo
# MAGIC
# MAGIC 1- Métrica de AUC (Área sob a curva ROC)
# MAGIC
# MAGIC 2- Previsão binária e matriz de confusão
# MAGIC
# MAGIC 3- Relatório de Classificação
# MAGIC
# MAGIC 4- KS (Kolmogorov-Smirnov)
# MAGIC
# MAGIC 5- Importância das variáveis na predição (Coeficientes de Regressão Logística)

# COMMAND ----------

#Curva ROC e cálculo de AUC
auc = roc_auc_score(y_test, y_pd)
print(f'AUC: {auc}')

#Previsão binária e Matriz de Confusão
y_pd_pred = (y_pd >= 0.5).astype(int)
confusion_matrix(y_test, y_pd_pred)

#Relatório de Classificação
print(classification_report(y_test, y_pd_pred))

#KS
dados_ks = pd.DataFrame({
    "y": y_test.values,
    "proba": y_pd
})

dados_ks["faixa"] = pd.qcut(dados_ks["proba"], 10, duplicates="drop")

tb_ks = dados_ks.groupby("faixa", observed=False).agg(
    total=("y", "count"),
    bons=("y", lambda x: (x == 0).sum()),
    maus=("y", lambda x: (x == 1).sum())
)

tb_ks["%bons_acum"] = tb_ks["bons"].cumsum() / tb_ks["bons"].sum()
tb_ks["%maus_acum"] = tb_ks["maus"].cumsum() / tb_ks["maus"].sum()

ks = max(abs(tb_ks["%bons_acum"] - tb_ks["%maus_acum"]))
print(f"KS: {ks}")

#Coeficientes
coef = pd.DataFrame({
    "variavel": X.columns,
    "coeficiente": modelo_pd.coef_[0]
}).sort_values(by="coeficiente", ascending=False)

coef


# COMMAND ----------

