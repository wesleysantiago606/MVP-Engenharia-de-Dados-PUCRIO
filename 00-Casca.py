# Databricks notebook source
# MAGIC %md
# MAGIC Notebook contendo as variáveis globais do fluxo

# COMMAND ----------

#Tabelas

base_credito = "default.base_credito"
base_outliers = "default.base_outliers"
base_clean = "default.base_clean"

#Imports
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql.window import Window
from pyspark.sql.types import NumericType
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, confusion_matrix, classification_report, roc_curve
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

#Variáveis do modelo

target = "loan_status"

var_num = [
    "person_age", 
    "person_income", 
    "person_emp_length",
    "loan_amnt", 
    "loan_int_rate", 
    "loan_percent_income",
    "cb_person_cred_hist_length",
    "cb_person_default_on_file"
]

var_cat = [
    "person_home_ownership", 
    "loan_intent", 
    "loan_grade"
]



