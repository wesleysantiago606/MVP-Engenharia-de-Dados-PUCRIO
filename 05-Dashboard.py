# Databricks notebook source
# MAGIC %md
# MAGIC Coletando as métricas definidas em 04-Modelo, cria-se um Dashboard com os gráficos de análise do modelo

# COMMAND ----------

# MAGIC %run "./00-Casca"

# COMMAND ----------

#Curva Roc
fpr, tpr, _ = roc_curve(y_test, y_pd)

plt.figure()
plt.plot(fpr, tpr, label="ROC")
plt.plot([0, 1], [0, 1], linestyle="--")
plt.xlabel("Taxa de Falsos Positivos")
plt.ylabel("Taxa de Verdadeiros Positivos")
plt.title("Curva ROC - Modelo de PD")
plt.legend()
plt.grid(True)
plt.show()

# COMMAND ----------

#Distrubuição das PDs
df_plot = pd.DataFrame({
    "Real": y_test.values,
    "PD": y_pd
})

plt.figure()
df_plot[df_plot["Real"] == 0]["PD"].hist(bins=30, alpha=0.6)
df_plot[df_plot["Real"] == 1]["PD"].hist(bins=30, alpha=0.6)
plt.xlabel("Probabilidade de Default")
plt.ylabel("Frequência")
plt.title("Distribuição das PDs - Bons vs Maus")
plt.grid(True)
plt.show()

# COMMAND ----------

# Matriz de Confusão
from sklearn.metrics import confusion_matrix

cm = confusion_matrix(y_test, y_pd_pred)

plt.figure()
plt.imshow(cm)
plt.colorbar()
plt.title("Matriz de Confusão")
plt.xlabel("Previsto")
plt.ylabel("Real")

for i in range(cm.shape[0]):
    for j in range(cm.shape[1]):
        plt.text(j, i, cm[i, j], ha="center", va="center")

plt.show()


# COMMAND ----------

# Curva KS
plt.figure()
plt.plot(tb_ks["%bons_acum"].values, label="Bons Acumulado")
plt.plot(tb_ks["%maus_acum"].values, label="Maus Acumulado")
plt.title("Curva KS")
plt.xlabel("Decis")
plt.ylabel("Distribuição Acumulada")
plt.legend()
plt.grid(True)
plt.show()

# COMMAND ----------

# Coeficientes das Variáveis
coef_plot = coef.sort_values(by="coeficiente")

plt.figure(figsize=(8, 10))
plt.barh(coef_plot["variavel"], coef_plot["coeficiente"])
plt.title("Importância das Variáveis no Modelo de PD")
plt.xlabel("Coeficiente")
plt.grid(True)
plt.show()