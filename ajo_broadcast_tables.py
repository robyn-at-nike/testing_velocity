--------------------------------------------------------------------------------------------------------------------------------------------
## Test: filter on dol table to shrink it down, also add a "temp" table in the form of a dataframe
--------------------------------------------------------------------------------------------------------------------------------------------
WITH view_dol AS 

  (
    SELECT
        dol.upm_id
        ,SUM(dol.grd_amt_excl_tax_usd) AS demand
        ,COUNT(DISTINCT dol.order_nbr) AS orders
        ,SUM(dol.origl_ordered_qty) AS units
    FROM dtc_integrated.dtc_digital_order_line dol
    WHERE dol.region_key = 1
        AND dol.rec_excl_ind = 0
        AND dol.ttl_demand_ind = 1
        AND UPPER(dol.univ_cat_desc) <> 'CONVERSE'
        AND dol.upm_id IS NOT NULL 
        AND (dol.rtn_qty = 0 OR dol.rtn_qty IS NULL)
    GROUP BY dol.upm_id

) 
JOIN test_control_table tct ON view_dol.upm_id = tct.upm_id

#### test combo

WITH view_dol AS 

  ( SELECT * FROM test_control_table
    SELECT
        dol.upm_id
        ,SUM(dol.grd_amt_excl_tax_usd) AS demand
        ,COUNT(DISTINCT dol.order_nbr) AS orders
        ,SUM(dol.origl_ordered_qty) AS units
    FROM dtc_integrated.dtc_digital_order_line dol
    WHERE dol.region_key = 1
        AND dol.rec_excl_ind = 0
        AND dol.ttl_demand_ind = 1
        AND UPPER(dol.univ_cat_desc) <> 'CONVERSE'
        AND dol.upm_id IS NOT NULL 
        AND (dol.rtn_qty = 0 OR dol.rtn_qty IS NULL)
    GROUP BY dol.upm_id

) 

#### converting SQL table to PySpark Df (to then maybe be broadcast joined)

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("test_control_table").getOrCreate()

# Assuming the Hive variables are set as Python variables
node_tst_str = "your_test_string_here"  
node_ctl_str = "your_control_string_here" 
aud_table = "your_aud_table_here"  
msmt_start_dt = "your_measurement_start_date_here"  
msmt_end_dt = "your_measurement_end_date_here"  

# Load the tables as DataFrames
aud_df = spark.table(aud_table)
bot_df = spark.table("gwan13.bot_master_nike_com")
slr_df = spark.table("aud_select_workspace.resellers")

# Perform the transformation
test_control_table = (aud_df
    .withColumn("test_control", 
        F.when(F.col("final_exposure_or_holdout").like(node_tst_str), F.lit("test"))
         .when(F.col("final_exposure_or_holdout").like(node_ctl_str), F.lit("control"))
         .otherwise(F.lit("na")))
    .join(bot_df, aud_df.upm_id == bot_df.upm_id, "left_anti")
    .join(slr_df, aud_df.upm_id == slr_df.upm_id, "left_anti")
    .filter((F.col("timestamp") >= msmt_start_dt) & (F.col("timestamp") <= msmt_end_dt))
    .withColumn("first_send_dt", F.min(F.substring(F.col("timestamp"), 1, 10)).over(Window.partitionBy("upm_id", "test_control")))
    .groupBy("upm_id", "test_control")
    .agg(F.min("first_send_dt").alias("first_send_dt"))
);


## Combining test_control_table as a PySpark df with SQL for DOL table

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, expr, col, countDistinct, avg, stddev, sum

# Initialize Spark Session
spark = SparkSession.builder.appName("test_control_table").getOrCreate()

# Set the Hive variables as Python variables

node_tst_str = "your_test_string_here"  
node_ctl_str = "your_control_string_here"  
aud_table = "your_aud_table_here"  
msmt_start_dt = "your_measurement_start_date_here" 
msmt_end_dt = "your_measurement_end_date_here" 

# SQL command for test_control_table
sql_command_test_control_table = f"""
    SELECT aud.upm_id,
           CASE
               WHEN final_exposure_or_holdout LIKE '{node_tst_str}' THEN 'test'
               WHEN final_exposure_or_holdout LIKE '{node_ctl_str}' THEN 'control'
               ELSE 'na'
           END AS test_control,
           MIN(LEFT(timestamp, 10)) as first_send_dt
    FROM {aud_table} aud
    LEFT ANTI JOIN gwan13.bot_master_nike_com bot ON aud.upm_id = bot.upm_id
    LEFT ANTI JOIN aud_select_workspace.resellers slr ON aud.upm_id = slr.upm_id
    WHERE timestamp BETWEEN '{msmt_start_dt}' AND '{msmt_end_dt}'
    GROUP BY 1, 2
"""
spark.sql(sql_command_test_control_table).createOrReplaceTempView("test_control_table")

# SQL command for view_dol
sql_command_view_dol = """
    SELECT dol.upm_id,
           SUM(dol.grd_amt_excl_tax_usd) AS demand,
           COUNT(DISTINCT dol.order_nbr) AS orders,
           SUM(dol.origl_ordered_qty) AS units
    FROM dtc_integrated.dtc_digital_order_line dol
    INNER JOIN test_control_table tct ON dol.upm_id = tct.upm_id
    WHERE dol.region_key = 1
      AND dol.rec_excl_ind = 0
      AND dol.ttl_demand_ind = 1
      AND UPPER(dol.univ_cat_desc) <> 'CONVERSE'
      AND dol.upm_id IS NOT NULL
      AND CAST(dol.order_dt AS DATE) BETWEEN DATE_SUB(tct.first_send_dt, 31) AND DATE_SUB(tct.first_send_dt, 1)
      AND (dol.rtn_qty = 0 OR dol.rtn_qty IS NULL)
    GROUP BY dol.upm_id
"""
spark.sql(sql_command_view_dol).createOrReplaceTempView("view_dol")

# Load test_control_table as DataFrame, so that it replacates a sql temp table
test_control_df = spark.table("test_control_table")

# Perform a broadcast join with the view_dol SQL view
joined_df = spark.table("view_dol").join(broadcast(test_control_df), "upm_id")

# Continue with further DataFrame transformations


# Implement the equivalent of the 'demand_bucket' and final SELECT query
# Use joined_df for these operations

