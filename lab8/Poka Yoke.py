# Databricks notebook source
# MAGIC %md
# MAGIC 5 metod, które mogą być użyte w Pipeline tak aby tyły odporne na błędy użytkownika, jak najbardziej „produkcyjnie”

# COMMAND ----------

# metoda 1 validate_input_schema

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def validate_input_schema(df, expected_schema: StructType):
    actual_schema = df.schema
    if actual_schema != expected_schema:
        raise ValueError(f"Nieprawidłowy schemat danych.\nOczekiwano: {expected_schema}\nOtrzymano: {actual_schema}")

# użycie

expected_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
validate_input_schema(df, expected_schema)


# COMMAND ----------

# metoda 2 check_nulls

def check_nulls(df, critical_columns):
    for col in critical_columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            raise ValueError(f"Kolumna '{col}' zawiera {null_count} brakujących wartości!")

# uzycie 
check_nulls(df, ["id", "name"])


# COMMAND ----------

# metoda 3 limit_data_volume
def limit_data_volume(df, max_rows=1_000_000):
    row_count = df.count()
    if row_count > max_rows:
        raise ValueError(f"Zbyt duży zbiór danych: {row_count} wierszy (limit: {max_rows})")

# użycie
limit_data_volume(df)


# COMMAND ----------

# metoda 4 enforce_unique_keys

def enforce_unique_keys(df, key_columns):
    total = df.count()
    unique = df.select(key_columns).dropDuplicates().count()
    if total != unique:
        raise ValueError("Klucz główny nie jest unikalny!")

# uzycie 
enforce_unique_keys(df, ["id"])


# COMMAND ----------

# metoda 5 log_step_and_time

import time
import logging

def log_step_and_time(step_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            logging.info(f"Start: {step_name}")
            result = func(*args, **kwargs)
            duration = time.time() - start
            logging.info(f"Koniec: {step_name} – czas: {duration:.2f}s")
            return result
        return wrapper
    return decorator


# użycie

@log_step_and_time("Join danych użytkowników")
def join_data(df1, df2):
    return df1.join(df2, "user_id")
