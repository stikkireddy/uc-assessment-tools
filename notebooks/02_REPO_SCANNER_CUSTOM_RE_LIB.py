# Databricks notebook source
# MAGIC %sh
# MAGIC sudo apt install -y libre2-dev pybind11-dev

# COMMAND ----------

# MAGIC %pip install pyre2
# MAGIC %pip install git+https://github.com/stikkireddy/uc-assessment-tools.git@main

# COMMAND ----------

# noinspection PyUnresolvedReferences
dbutils.library.restartPython()

# COMMAND ----------

# noinspection PyPep8
from assessment.ui.app import Page

# COMMAND ----------

Page()

# COMMAND ----------


