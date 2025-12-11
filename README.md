# MVP - Engenharia de Dados - PUC Rio

Este repositório contém os scripts de um pipeline completo de um Modelo de Probabilidade de Default de uma carteira de crédito

## Objetivo do Projeto

- Construir um pipeline de dados com arquitetura em camadas (Bronze, Silver e Gold)
- Realizar análise de qualidade dos dados
- Desenvolver um modelo de Probabilidade de Default com Regressão Logística
- Avaliar o modelo por meio de AUC, KS, Curva ROC, Matriz de Confusão e análise de importância das variáveis
- Disponibilizar um dashboard final com visualizações dos resultados.

## Arquitetura da Solução (Lakehouse)

- **Bronze** → Dados brutos (`base_credito`)
- **Silver** → Dados tratados (`base_clean`)
- **Gold** → Base final para modelagem de PD (`base_modelo`)

##  Carga da Base `cr_loan2.csv` (Camada Bronze)

A base de dados utilizada no projeto é pública e foi obtida a partir da plataforma Kaggle. Para garantir a reprodutibilidade do projeto, siga os passos abaixo.

### 1. Download da Base no Kaggle

1. Acesse: https://www.kaggle.com/datasets/arunbhuta/credit-analysis-probability-of-default
2. Faça o download do arquivo (normalmente em formato `.csv`)
3. Extraia o arquivo no seu computador, caso esteja compactado
4. O arquivo utilizado no projeto deve se chamar, por exemplo: `cr_loan2.csv`

### 2. Upload do Arquivo e Criação da Tabela no Databricks (via UI)

1. No Databricks, acesse o Workspace do seu projeto
2. No menu lateral esquerdo, clique em: Data → Add Data → Upload File
3. Clique em "Click to browse" ou arraste o arquivo cr_loan2.csv
4. Após o upload, o Databricks mostrará a opção de "Create Table"
5. Na tela de criação de tabela:
   
  - Data source: o arquivo que você acabou de enviar
    
  - Table name: base_credito
    
  - Database: default
    
  - File type: CSV
    
  - Marcar a opção "First row is header"
    
6. Clique em Create Table

Isso criará a tabela: default.base_credito que é utilizada como camada Bronze no pipeline.

### 3. Validação da Tabela Criada

Você pode validar a criação da tabela de duas formas:

Via interface:

Clique em Data

Abra o catálogo default

Verifique se a tabela base_credito aparece na lista

Clique nela e visualize algumas linhas

Via notebook (opcional): spark.table("default.base_credito").show(5)

---

## Como Executar o Projeto no Databricks

### 1. Criar o ambiente

1. Criar uma conta no **Databricks Free Edition**
2. Criar um **cluster**
3. Criar uma pasta no Workspace para o projeto (ex: `MVP_PD`)


### 2. Importar os notebooks

Para cada arquivo `.py` deste repositório:

1. Acesse o Databricks
2. Vá em: Workspace → Sua pasta → Import
3. Faça upload de cada arquivo `.py`
4. Confirme que os nomes dos notebooks ficaram exatamente:

- `0-Orquestrador`
- `00-Casca`
- `01-Exploração dos Dados`
- `02-Tratamento dos Dados`
- `03-Validação dos Dados`
- `04-Modelo`
- `05-Dashboard`


###  3. Ajustar caminhos no Orquestrador

No notebook `0-Orquestrador`, ajuste os caminhos dos `%run` para refletir a sua pasta do Workspace, por exemplo:

```python
%run "/Workspace/Users/seu_email@databricks.com/MVP_PD/00-Config"
```

### 4. Executar o Pipeline Completo

1. Abra o notebook: 0-Orquestrador
2. Run → Run All

O pipeline irá executar automaticamente:

1. Exploração dos dados
2. Tratamento (outliers + nulos)
3. Validação
4. Treinamento do modelo
5. Avaliação
6. Geração do dashboard

### 5. Visualizar o Dashboard

Após a execução do notebook 05-Dashboard clique em: View → Present e navegue entre os gráficos:

-Curva ROC e AUC
-Distribuição das PDs
-Matriz de Confusão
-Curva KS
-Importância das Variáveis

---

## Tecnologias Utilizadas

- Databricks Free Edition
- PySpark
- Pandas
- NumPy
- Scikit-learn
- Matplotlib
- MLflow (opcional)

---

## Observações Importantes

- Este projeto tem finalidade acadêmica
- A base de dados utilizada é pública
- O modelo não deve ser utilizado diretamente em produção sem ajustes adicionais
- O threshold de decisão pode ser calibrado conforme o apetite de risco

---

## Autor: Wesley Ramos Neres Santiago

-LinkedIn: https://www.linkedin.com/in/wesley-santiago-165b78186/

## Licença

Uso livre para fins educacionais.

