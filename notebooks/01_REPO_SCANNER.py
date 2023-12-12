# Databricks notebook source
# MAGIC %pip install git+https://github.com/stikkireddy/uc-assessment-tools.git@main dbtunnel

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from dbtunnel import dbtunnel

from assessment.ui import app

# COMMAND ----------

dbtunnel.solara(app.__file__) \
  .inject_auth() \
  .inject_env(**{"EXPERIMENTAL_FIND_AND_REPLACE": "true"}) \
  .run()

# COMMAND ----------


