#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SaudeBliss — Code Challenge (versão refinada e aderente ao README)

O pipeline:
- Lê transactions.json e payments.json com schemas explícitos (modo PERMISSIVE)
- Faz parsing robusto de datas (vários formatos suportados)
- Consolida múltiplos pagamentos por transação (sum, last_payment_dt, max_days_late)
- Calcula inadimplência por CLIENTE usando janelas:
    * Critério 1: algum atraso >30 dias em janela de 3 meses (rolling)
    * Critério 2: algum atraso >15 dias em janela de 6 meses (rolling)
- Gera dois conjuntos Parquet:
    * transactions_enriched/
    * mart_delinquency_by_customer/  (particionado por processing_date e inadimplente=true|false)
- Parametrização via CLI; compressão Snappy
"""

import argparse
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


TRANSACTIONS_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("customer_id",   T.StringType()),
    T.StructField("amount",        T.DoubleType()),
    T.StructField("currency",      T.StringType()),
    T.StructField("transaction_date", T.StringType()),
    T.StructField("due_date",         T.StringType()),
    T.StructField("payment_terms", T.StringType()),
    T.StructField("status",        T.StringType()),
    T.StructField("metadata", T.StructType([
        T.StructField("product_category",  T.StringType()),
        T.StructField("sales_rep",         T.StringType()),
        T.StructField("channel",           T.StringType()),
        T.StructField("discount_applied",  T.IntegerType()),
        T.StructField("installments",      T.IntegerType()),
    ]))
])

PAYMENTS_SCHEMA = T.StructType([
    T.StructField("payment_id",     T.StringType()),
    T.StructField("transaction_id", T.StringType()),
    T.StructField("customer_id",    T.StringType()),
    T.StructField("amount_paid",    T.DoubleType()),
    T.StructField("currency",       T.StringType()),
    T.StructField("payment_date",   T.StringType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("payment_type",   T.StringType()),
    T.StructField("status",         T.StringType()),
    T.StructField("metadata", T.StructType([
        T.StructField("processor",        T.StringType()),
        T.StructField("fee_amount",       T.DoubleType()),
        T.StructField("reference_number", T.StringType()),
        T.StructField("ip_address",       T.StringType()),
        T.StructField("user_agent",       T.StringType()),
    ]))
])

SUPPORTED_DATE_FORMATS = [
    "yyyy-MM-dd",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ssXXX",
    "yyyy-MM-dd HH:mm:ss",
]


def parse_args():
    p = argparse.ArgumentParser(description="SaudeBliss Code Challenge — Spark ETL")
    p.add_argument("--transactions", required=True, help="Caminho do transactions.json")
    p.add_argument("--payments",     required=True, help="Caminho do payments.json")
    p.add_argument("--outdir",       required=True, help="Diretório base de saída")
    p.add_argument("--mode", default="overwrite", choices=["overwrite","append","error","ignore"],
                   help="Modo de escrita")
    p.add_argument("--enriched_partitions", default="processing_date",
                   help="Colunas de particionamento do dataset enriched (separe por vírgula)")
    # No mart usaremos 'processing_date,inadimplente' por padrão
    p.add_argument("--mart_partitions", default="processing_date,inadimplente",
                   help="Colunas de particionamento do mart (separe por vírgula)")
    p.add_argument("--repartition", type=int, default=0, help="Reparticionar antes de escrever (0 = não)")
    return p.parse_args()


def create_spark(app_name: str = "SaudeBlissCodeChallengeRefined") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def parse_any_timestamp(col: F.Column) -> F.Column:
    """Tenta converter string em timestamp usando vários formatos suportados."""
    expr = None
    for fmt in SUPPORTED_DATE_FORMATS:
        candidate = F.to_timestamp(col, fmt)
        expr = candidate if expr is None else F.coalesce(expr, candidate)
    return expr  # pode retornar null se nenhum formato casar


def read_transactions(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read
        .schema(TRANSACTIONS_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(path)
    )
    df = df.withColumn("transaction_ts", parse_any_timestamp(F.col("transaction_date"))) \
           .withColumn("due_ts",          parse_any_timestamp(F.col("due_date"))) \
           .withColumn("transaction_dt", F.to_date(F.col("transaction_ts"))) \
           .withColumn("due_dt",         F.to_date(F.col("due_ts")))
    return df


def read_payments(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read
        .schema(PAYMENTS_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(path)
    )
    df = df.withColumn("payment_ts", parse_any_timestamp(F.col("payment_date"))) \
           .withColumn("payment_dt", F.to_date(F.col("payment_ts")))
    return df


def build_enriched(trans: DataFrame, pays: DataFrame) -> DataFrame:
    p = pays.select(
        "payment_id", "transaction_id", F.col("customer_id").alias("payment_customer_id"),
        "amount_paid", F.col("currency").alias("payment_currency"),
        "payment_method", "payment_type", F.col("status").alias("payment_status"),
        "metadata".alias("payment_metadata"), "payment_ts", "payment_dt"
    )

    joined = (
        trans.alias("t").join(p.alias("p"), on="transaction_id", how="left")
        .withColumn("days_late",
            F.when(F.col("p.payment_dt").isNotNull(),
                   F.datediff(F.col("p.payment_dt"), F.col("t.due_dt")))
        )
    )

    agg = (
        joined.groupBy("transaction_id")
        .agg(
            F.max("p.payment_dt").alias("last_payment_dt"),
            F.max("days_late").alias("max_days_late"),
            F.min("days_late").alias("min_days_late"),
            F.sum(F.coalesce(F.col("p.amount_paid"), F.lit(0.0))).alias("total_paid"),
            F.countDistinct("p.payment_id").alias("payments_count"),
        )
    )

    enriched = (
        trans.join(agg, on="transaction_id", how="left")
        .withColumn("event_dt", F.coalesce(F.col("last_payment_dt"), F.col("due_dt")))
        .withColumn("amount_outstanding",
                    F.when(F.col("amount").isNotNull(),
                           F.col("amount") - F.coalesce(F.col("total_paid"), F.lit(0.0))))
        .withColumn("late_over_30", F.col("max_days_late") > F.lit(30))
        .withColumn("late_over_15", F.col("max_days_late") > F.lit(15))
    )
    return enriched


def build_customer_mart(enriched: DataFrame) -> DataFrame:
    df = enriched.withColumn("event_ts", F.unix_timestamp(F.col("event_dt")))

    # janelas deslizantes por cliente (em segundos)
    w3m = Window.partitionBy("customer_id").orderBy(F.col("event_ts")).rangeBetween(-90*24*3600, 0)
    w6m = Window.partitionBy("customer_id").orderBy(F.col("event_ts")).rangeBetween(-180*24*3600, 0)

    mart = (
        df
        .withColumn("late30_in_3m", F.sum(F.col("late_over_30").cast("int")).over(w3m))
        .withColumn("late15_in_6m", F.sum(F.col("late_over_15").cast("int")).over(w6m))
        .withColumn("delinquent_crit1", F.col("late30_in_3m") > F.lit(0))
        .withColumn("delinquent_crit2", F.col("late15_in_6m") > F.lit(0))
        .withColumn("delinquent", F.col("delinquent_crit1") | F.col("delinquent_crit2"))
        .groupBy("customer_id")
        .agg(
            F.max("delinquent_crit1").alias("delinquent_crit1"),
            F.max("delinquent_crit2").alias("delinquent_crit2"),
            F.max("delinquent").alias("delinquent"),
            F.max("amount_outstanding").alias("max_amount_outstanding"),
            F.max("max_days_late").alias("max_days_late"),
            F.countDistinct("transaction_id").alias("transactions"),
        )
    )
    return mart


def write_df(df: DataFrame, outpath: str, mode: str, partitions: str):
    cols = [c.strip() for c in partitions.split(',') if c.strip()]
    writer = (
        df.write
        .mode(mode)
        .option("compression", "snappy")
    )
    if cols:
        writer = writer.partitionBy(*cols)
    writer.parquet(outpath)


def main():
    args = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    processing_date = F.current_date()

    # 1) Read
    trans = read_transactions(spark, args.transactions)
    pays  = read_payments(spark, args.payments)

    # 2) Enrich
    enriched = build_enriched(trans, pays) \
        .withColumn("processing_date", processing_date)

    if args.repartition > 0:
        enriched = enriched.repartition(args.repartition)

    # 3) Customer mart
    mart = build_customer_mart(enriched) \
        .withColumn("processing_date", processing_date) \
        .withColumn("inadimplente", F.col("delinquent"))  # alias solicitado pelo README

    # 4) Write
    write_df(
        enriched,
        outpath=f"{args.outdir}/transactions_enriched",
        mode=args.mode,
        partitions=args.enriched_partitions,
    )
    write_df(
        mart,
        outpath=f"{args.outdir}/mart_delinquency_by_customer",
        mode=args.mode,
        partitions=args.mart_partitions,  # já inclui processing_date,inadimplente
    )

    # 5) Quick counters (útil para logs)
    n_trans = trans.count()
    n_pays  = pays.count()
    n_enr   = enriched.count()
    n_mart  = mart.count()

    print("\n===== SUMMARY =====")
    print(f"transactions: {n_trans}")
    print(f"payments:     {n_pays}")
    print(f"enriched:     {n_enr}")
    print(f"mart:         {n_mart}")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
