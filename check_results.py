#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_results.py — Ajuda a avaliar a saída do pipeline
- Percentual de clientes inadimplentes
- Lista de transações que tiveram atrasos que compõem violação
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("saudebliss_check").getOrCreate()

# Caminhos padrão (ajuste se necessário)
MART_PATH = "output/mart_delinquency_by_customer"
ENR_PATH  = "output/transactions_enriched"

# 1) Percentual de clientes inadimplentes
mart = spark.read.parquet(MART_PATH)

tot = mart.select("customer_id").distinct().count()
delinq = mart.filter(F.col("inadimplente") == True).select("customer_id").distinct().count()

pct = 0.0 if tot == 0 else (delinq / tot) * 100.0
print("==== Percentual de inadimplentes ====")
print(f"Clientes (total): {tot}")
print(f"Inadimplentes:   {delinq}  ({pct:.2f}%)")

# 2) Transações com atrasos que violam critérios
enriched = spark.read.parquet(ENR_PATH)
viol = (
    enriched
    .select("customer_id", "transaction_id", "last_payment_dt", "max_days_late", "amount", "total_paid")
    .withColumn("violou_crit1", F.col("max_days_late") > 30)  # >30 em 3m
    .withColumn("violou_crit2", F.col("max_days_late") > 15)  # >15 em 6m
    .filter("violou_crit1 or violou_crit2")
)

print("\n==== Transações com possível violação de critério ====")
viol.orderBy(F.desc("max_days_late")).show(50, truncate=False)

spark.stop()
