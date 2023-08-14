# Databricks notebook source
# MAGIC %pip install git+https://github.com/stikkireddy/uc-assessment-tools.git@main

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from assessment.ui.app import Page

# COMMAND ----------

Page()
