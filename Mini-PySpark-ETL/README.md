# Mini Project: PySpark ETL  
**Use case: Banking — Daily Card Transactions → Curated Delta Table**

---

### 0) Setup (paths & table names)

```python
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType

# Source: raw landing zone (CSV/JSON/Parquet). Replace with your path (ADLS/S3/GCS)
SRC_PATH   = "abfss://landing@yourstorageaccount.dfs.core.windows.net/cards/transactions/dt=2025-09-08/"

# Reference data (e.g., FX rates) – optional
FX_PATH    = "abfss://ref@yourstorageaccount.dfs.core.windows.net/fx/daily_rates/"

# Target: curated Delta table (gold layer)
TGT_DB     = "bank_curated"
TGT_TABLE  = "card_transactions_daily"
TGT_PATH   = "abfss://curated@yourstorageaccount.dfs.core.windows.net/cards/transactions_daily/"

```

### 1) **EXTRACT** (read raw data with an explicit schema)

```python
txn_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id",   StringType(), True),
    StructField("amount",        DoubleType(), True),
    StructField("currency",      StringType(), True),    # e.g., 'USD','CAD','EUR'
    StructField("merchant_cat",  StringType(), True),    # e.g., 'grocery','fuel'
    StructField("txn_ts",        StringType(), True),    # raw string timestamp
    StructField("device_country",StringType(), True),
    StructField("home_country",  StringType(), True)
])

raw_txn = (spark.read
           .option("header", "true")
           .schema(txn_schema)
           .csv(SRC_PATH))

```

### 2) **TRANSFORM** (clean, normalize, enrich, deduplicate)

### a) Parse timestamp & basic validation
```python
tx = (raw_txn
      .withColumn("txn_time", F.to_timestamp("txn_ts"))
      .drop("txn_ts")
      .filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
      .filter(F.col("txn_time").isNotNull()))
```

---

### b) Deduplicate on strongest business key
```python
tx = tx.dropDuplicates(["transaction_id"])
```
---

### c) Normalize currency (join to FX table to convert to a base currency, e.g., USD)

```python
fx = (spark.read.format("delta").load(FX_PATH))  # or csv/parquet; expect cols: currency, rate_to_usd

# If you don’t have FX, comment the next 3 lines and keep 'amount_usd' = amount
tx = (tx.join(fx, on="currency", how="left")
        .withColumn("amount_usd", F.round(F.col("amount") * F.coalesce(F.col("rate_to_usd"), F.lit(1.0)), 2))
        .drop("rate_to_usd"))
```
---
### d) Derive simple risk signals (toy example for downstream fraud / BI)

```python
tx = (tx
      .withColumn("is_cross_border", (F.col("device_country") != F.col("home_country")).cast("int"))
      .withColumn("hour_of_day", F.hour("txn_time"))
      .withColumn("high_amount_flag", (F.col("amount_usd") > 5000).cast("int"))
      .withColumn("risk_score",
                  F.col("high_amount_flag")*2 + F.col("is_cross_border") +
                  F.when((F.col("hour_of_day")>=0) & (F.col("hour_of_day")<=5), 1).otherwise(0))
     )
```

### 3) **LOAD** (write to a Delta table, partitioned by date for performance)
### a) Partition column (YYYY-MM-DD)

```python
tx = tx.withColumn("txn_date", F.to_date("txn_time"))
```

### b) # Create database if needed


```python
spark.sql(f"CREATE DATABASE IF NOT EXISTS {TGT_DB} LOCATION 'abfss://curated@yourstorageaccount.dfs.core.windows.net/'")
```

### c) Write as Delta (append daily)
### Write as Delta (append daily)

```python
(tx.repartition("txn_date")               # good shuffle boundary
   .write
   .format("delta")
   .mode("append")
   .partitionBy("txn_date")
   .option("overwriteSchema", "true")
   .save(TGT_PATH))
```

### d) # Register/refresh the table in the metastore

### Register/refresh the table in the metastore

```python
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TGT_DB}.{TGT_TABLE}
USING DELTA
LOCATION '{TGT_PATH}'
""")

spark.sql(f"MSCK REPAIR TABLE {TGT_DB}.{TGT_TABLE}")  # load partitions (Hive-compatible)
```

## 4) (Optional) Optimize & Vacuum (Delta maintenance)

```python
spark.sql(f"OPTIMIZE {TGT_DB}.{TGT_TABLE} ZORDER BY (customer_id, txn_time)")

# Vacuum safely (default retention 7 days; adjust only if you understand the implications)
# spark.sql(f"VACUUM {TGT_DB}.{TGT_TABLE} RETAIN 168 HOURS")   # 7 days
```

### 5) Quick validation query (what your dashboard would hit)


```python
spark.sql(f"""
SELECT txn_date,
       merchant_cat,
       COUNT(*) as txn_cnt,
       SUM(amount_usd) as total_usd,
       SUM(CASE WHEN risk_score >= 3 THEN 1 ELSE 0 END) as high_risk_cnt
FROM {TGT_DB}.{TGT_TABLE}
GROUP BY txn_date, merchant_cat
ORDER BY txn_date DESC, total_usd DESC
""").show(20, truncate=False)

