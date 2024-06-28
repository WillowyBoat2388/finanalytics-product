from dagster import (
                    graph_multi_asset,
                    AssetOut,
                    AssetKey,
                    In,
                    Out,
                    op,
                    DynamicOut,
                    DynamicOutput,
                    OpExecutionContext,
                    RetryPolicy,
                    Backoff,
                    Jitter
                )
from typing import Dict, List
from dagster_deltalake import DeltaLakePyarrowIOManager, S3Config
# from ..resources import MyPysparkResource
from dagster_pyspark import PySparkResource
from datetime import datetime as dt
import json
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import *
from pyspark.sql import functions as F

@op(
    out=DynamicOut()
)
def stock_tables(context, stock_data):
    instance = context.instance
    materialization = instance.get_latest_materialization_event(AssetKey(["finnhub_US_stocks"])).asset_materialization
    
    mtdt = list(materialization.metadata.keys())
    
    mping_keys = [i.split(".")[0] if "." in i else i[:-5] for i in mtdt ]
    
    i = 0
    for data in stock_data:
        yield(DynamicOutput(data, mapping_key=mping_keys[i]))
        i += 1

@op()
def get_result_dataframe(dataframe, symbol, spark):
    # Repartition the DataFrames to have the same number of partitions
    if dataframe.rdd.getNumPartitions() != symbol.rdd.getNumPartitions():
        num_partitions = min(dataframe.rdd.getNumPartitions(), symbol.rdd.getNumPartitions())
        df = dataframe.repartition(num_partitions)
        df_s = symbol.repartition(num_partitions)
    else:
        df = dataframe.repartition(1)
        df_s = symbol.repartition(1)
    schema = StructType(dataframe.schema.fields + symbol.schema.fields)
    
    df1df2 = df.rdd.zip(df_s.rdd).map(lambda x: x[0]+x[1])
    
    return spark.createDataFrame(df1df2, schema)

@op()
def get_array_names_in_struct(schema, parent_name=''):
    """
    Recursively get the names of array fields within a struct.

    :param schema: The schema of the struct or the entire DataFrame.
    :param parent_name: The name of the parent struct to prepend.
    :return: A list of the full names of the array fields.
    """
    array_names = []
    for field in schema.fields:
        full_name = f"{parent_name}.{field.name}" if parent_name else field.name
        if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            array_names.append(full_name)
        if isinstance(field.dataType, StructType):
            array_names.extend(get_array_names_in_struct(field.dataType, full_name))
    return array_names

@op()
def get_array_element_names(schema, parent_name=''):
    """
    Recursively get the names of elements in array fields within a struct.

    :param schema: The schema of the struct or the entire DataFrame.
    :param parent_name: The name of the parent struct to prepend.
    :return: A list of the full names of the array elements.
    """
    element_names = []
    for field in schema.fields:
        full_name = f"{parent_name}.{field.name}" if parent_name else field.name
        if isinstance(field.dataType, ArrayType):
            # If the array's element type is a struct, get the field names within it
            if isinstance(field.dataType.elementType, StructType):
                for nested_field in field.dataType.elementType.fields:
                    nested_full_name = f"{full_name}.{nested_field.name}"
                    element_names.append(nested_full_name)
            # else:
            #     # If the array's element type is not a struct, just add the array name
            #     element_names.append(full_name)
        elif isinstance(field.dataType, StructType):
            # Recursively process nested structs
            element_names.extend(get_array_element_names(field.dataType, full_name))
    return element_names



@op(
    required_resource_keys= {"pyspark": PySparkResource},
    out={'metric_and_series': Out(metadata= {"date": dt.now().strftime("%Y-%m-%d")}, io_manager_key="s3_prqt_io_manager"),
        },
    retry_policy=RetryPolicy(
        max_retries=3,
        delay=0.2,  # 200ms
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    ),
    tags = {
        'dagster-k8s/config': {
            'job_spec_config': {
                'ttl_seconds_after_finished': 60
            }
        }
    }
)
def create_stock_tables(context:OpExecutionContext, input_fn):
    try:
        pyspark = context.resources.pyspark
        spark = pyspark.spark_session
        sc = pyspark.spark_session.sparkContext
        mtrc_and_srs = []
        symbols = []
        dct = {}
        for key,value in input_fn.items():
            read_options = {
                "multiline": True,
                "mode": "PERMISSIVE",
                "dateFormat": "yyyy-MM-dd",
                "allowSingleQuotes": True
            }
            if key == 'metric':
                df = spark.read.options(**read_options).json(sc.parallelize([json.dumps(value)]))
                
                mtrc_and_srs.append(df)
                
                dct['mtrc'] = df.count()

                context.log.info(df.head())
                # context.log.info(df.count())
                # break
            
            elif key == 'series' and value:
                df = spark.read.options(**read_options).json(sc.parallelize([json.dumps(value)]))
                
                annual_schema = df.schema["annual"].dataType
                quarterly_schema = df.schema["quarterly"].dataType
                quarterly_f_columns = get_array_element_names(quarterly_schema, "quarterly")
                annual_f_columns = get_array_element_names(annual_schema, "annual")
                quarterly_columns = get_array_names_in_struct(quarterly_schema, "quarterly")
                annual_columns = get_array_names_in_struct(annual_schema, "annual")

                annual_zipped = F.arrays_zip(*[column for column in annual_columns])
                quarterly_zipped = F.arrays_zip(*[column for column in quarterly_columns])


                # Explode the annual zipped array to create rows for each period with all 'v' values
                df_annual_exploded = df.select(
                    F.explode(annual_zipped).alias("annual")
                ).select([F.col(cols).alias(f"{'.'.join(str.split(cols, '.')[:-1])}") if "period" not in cols else F.col(cols) for cols in annual_f_columns])

                
                df_annual_cols = df_annual_exploded.columns

                # Explode the quarterly zipped array to create rows for each period with all 'v' values
                df_quarterly_exploded = df.select(
                    F.explode(quarterly_zipped).alias("quarterly")
                ).select([F.col(cols).alias(f"{'.'.join(str.split(cols, '.')[:-1])}") if "period" not in cols else F.col(cols) for cols in quarterly_f_columns])

                df_quarterly_cols = df_quarterly_exploded.columns

                df_a_dupli_col_idx = [idx for idx, val in enumerate(df_annual_cols) if val == 'period']

                for i in df_a_dupli_col_idx:
                    df_annual_cols[i] = df_annual_cols[i] + '_duplicate_'+ str(i)

                df_q_dupli_col_idx = [idx for idx, val in enumerate(df_quarterly_cols) if val == 'period']

                for i in df_q_dupli_col_idx:
                    df_quarterly_cols[i] = df_quarterly_cols[i] + '_duplicate_'+ str(i)

                # Rename the duplicate columns in data frame
                df_a_e = df_annual_exploded.toDF(*df_annual_cols)

                a_date_columns = [c for c in df_a_e.columns if 'period' in c]
                a_non_date_columns = [c for c in df_a_e.columns if c not in a_date_columns]


                # Rename the duplicate columns in data frame
                df_q_e = df_quarterly_exploded.toDF(*df_quarterly_cols)

                q_date_columns = [c for c in df_q_e.columns if 'period' in c]
                q_non_date_columns = [c for c in df_q_e.columns if c not in q_date_columns]

                # Use coalesce to merge the date columns into a single consistent date column
                df_a_with_single_date = df_a_e.withColumn("date", F.coalesce(*[df_a_e[col] for col in df_a_e.columns if 'period' in col]))
                df_a_with_single_date = df_a_with_single_date.drop(*[col for col in a_date_columns])

                # Use coalesce to merge the date columns into a single consistent date column
                df_q_with_single_date = df_q_e.withColumn("date", F.coalesce(*[df_q_e[col] for col in df_q_e.columns if 'period' in col]))
                df_q_with_single_date = df_q_with_single_date.drop(*[col for col in q_date_columns])

                # df = df_q_with_single_date.join(df_a_with_single_date, on='date', how='full')
                quarterly_metrics = df_q_with_single_date
                annual_metrics = df_a_with_single_date

                mtrc_and_srs.append(quarterly_metrics)
                mtrc_and_srs.append(annual_metrics)
                dct['quarterly'] = quarterly_metrics.count()
                dct['annual'] = annual_metrics.count()

                context.log.info(quarterly_metrics.head())
                context.log.info(annual_metrics.head())
                # context.log.info(df.count())

            elif key == 'symbol':
                
                if 'mtrc' in dct.keys():
                    mtrcsymbl = [[value]] * dct['mtrc']
                    symbols.append(mtrcsymbl)

                if 'quarterly' in dct.keys() and 'annual' in dct.keys():
                    quarterlysymbl = [[value]] * dct['quarterly']
                    annualsymbl = [[value]] * dct['annual']

                    symbols.append(quarterlysymbl)
                    symbols.append(annualsymbl)


        
        # Create an empty schema
        # Define the schema
        schema = StructType([
            StructField("symbol", StringType(), True)
        ])
        
        if len(mtrc_and_srs) >= 2:
            mtrc = mtrc_and_srs[0]
            mtrcsymbol = spark.createDataFrame(sc.parallelize(symbols[0]), schema)
            qrtrly_mtrc = mtrc_and_srs[1]
            annual_mtrc = mtrc_and_srs[-1]
            qrtrlysymbol = spark.createDataFrame(sc.parallelize(symbols[1]), schema)
            annlsymbol = spark.createDataFrame(sc.parallelize(symbols[-1]), schema)
            
            mtrc.collect()
            qrtrly_mtrc.collect()
            annual_mtrc.collect()
            
            metric = get_result_dataframe(mtrc, mtrcsymbol, spark)
            # context.log.info(srs.count())
            # context.log.info(srssymbol.count())
            quarterly_metrics = get_result_dataframe(qrtrly_mtrc, qrtrlysymbol, spark)
            annual_metrics = get_result_dataframe(annual_mtrc, annlsymbol, spark)
        
            # Add a distinguishing column to each DataFrame
            df1_with_marker = metric.withColumn("type", F.lit("metric"))
            df2_with_marker = quarterly_metrics.withColumn("type", F.lit("quarterly_metric"))
            df3_with_marker = annual_metrics.withColumn("type", F.lit("annual_metric"))

            # Get the list of all columns in all DataFrames
            metric_columns = df1_with_marker.columns
            quarterly_columns = df2_with_marker.columns
            annual_columns = df3_with_marker.columns
            series_columns = quarterly_columns + ["srs_sep"] + [item for item in annual_columns if item not in quarterly_columns]
            all_columns = metric_columns + [item for item in series_columns if item not in metric_columns]

            df1 = df1_with_marker
            df2 = df2_with_marker
            df3 = df3_with_marker

            # Add missing columns with null values to each DataFrame
            for col in metric_columns:
                if col not in series_columns or col == "srs_sep":
                    df2 = df2.withColumn(col, F.lit(None).cast(StringType()))
                    df3 = df3.withColumn(col, F.lit(None).cast(StringType()))
            for col in series_columns:
                if col not in df2.columns:
                    df2 = df2.withColumn(col, F.lit(None).cast(StringType()))
                if col not in df3.columns:
                    df3 = df3.withColumn(col, F.lit(None).cast(StringType()))
            for col in series_columns:
                if col not in metric_columns:
                    df1 = df1.withColumn(col, F.lit(None).cast(StringType()))
            
            # Ensure the columns are in the same order
            df1 = df1.select(*[f"`{col}`" for col in all_columns])
            df2 = df2.select(*[f"`{col}`" for col in all_columns])
            df3 = df3.select(*[f"`{col}`" for col in all_columns])

            # Union the DataFrames together
            chained_df = df1.unionByName(df2)
            chained_df = chained_df.unionByName(df3)
        else:
            mtrc = mtrc_and_srs[0]
            mtrcsymbol = spark.createDataFrame(sc.parallelize(symbols[0]), schema)
            metric = get_result_dataframe(mtrc, mtrcsymbol, spark)

            chained_df = metric.withColumn("type", F.lit("metric"))

        # context.get_step_execution_context().step.key(f"create_tables_stock_{symbol}")
        chained_df.coalesce(1)

        return chained_df
    except Exception as e:
        context.log.info(f"Pyspark Error: {e}")

    

@op(
    required_resource_keys= {"pyspark": PySparkResource},
    out={"metric": Out(metadata= {"date": dt.now().strftime("%Y-%m-%d")}, io_manager_key="s3_prqt_io_manager"),
         "series": Out(metadata= {"date": dt.now().strftime("%Y-%m-%d")}, io_manager_key="s3_prqt_io_manager")
    },
    retry_policy=RetryPolicy(
        max_retries=2,
        delay=0.2,  # 200ms
        backoff=Backoff.EXPONENTIAL,
        jitter=Jitter.PLUS_MINUS,
    )
)
def merge_and_analyze(context, df_list):




    pyspark = context.resources.pyspark
    spark = pyspark.spark_session
    metric = []
    series = []
    
    # Create an empty schema
    schemas = {}

    mtrcschema = None
    srsschema = None
    
    try:
        for item in df_list:
            from pyspark.sql.functions import col, count

            if 'type' in item.columns:
                quarterly_count_df = item.select(count(F.when(col('type') == "quarterly_metric", 1)).alias('quarterly_count'))
                annual_count_df = item.select(count(F.when(col('type') == "annual_metric", 1)).alias('annual_count'))
                quarterly_count = quarterly_count_df.collect()[0]['quarterly_count']
                annual_count = annual_count_df.collect()[0]['annual_count']
                if annual_count > 0 and quarterly_count > 0:
            # if item.filter(item.type == "series").count() > 0:
                    mtrc = item.filter(item["type"] == "metric")
                    quarterly_metric = item.filter(item["type"] == "quarterly_metric")
                    annual_metric = item.filter(item["type"] == "annual_metric")

                    # Get the list of column names
                    all_columns = item.columns
                    
                    # Find the index of the column to keep
                    keep_column_index = all_columns.index("type")
                    srs_sep_column_index = all_columns.index("srs_sep")
                    
                    # Slice the list of columns to keep only the column to keep and its surrounding columns
                    srscolumns_to_keep = all_columns[keep_column_index - 1:-1]
                    mtrccolumns_to_keep = all_columns[:keep_column_index]
                    
                    # Find the index of the column to keep
                    srs_sep_column_index = srscolumns_to_keep.index("srs_sep")

                    # Slice the list of columns to keep only the column to keep and its surrounding columns
                    qrtrlycolumns_to_keep = srscolumns_to_keep[:srs_sep_column_index]
                    annlcolumns_to_keep = srscolumns_to_keep[srs_sep_column_index+1:-1]

                    mtrccolumns_to_keep = ["symbol"] + [col for col in mtrccolumns_to_keep if col != "symbol"]
                    qrtrlycolumns_to_keep = ["date", "symbol"] + [col for col in qrtrlycolumns_to_keep if col not in ["date", "symbol"]]
                    annlcolumns_to_keep = ["date", "symbol"] + annlcolumns_to_keep

                    # Select only the columns to keep
                    mtrc = mtrc.select(*[f"`{value}`" for value in mtrccolumns_to_keep])
                    quarterly_metric = quarterly_metric.select(*[f"`{value}`" for value in qrtrlycolumns_to_keep])
                    annual_metric = annual_metric.select(*[f"`{value}`" for value in annlcolumns_to_keep])

                    quarterly_metric = quarterly_metric.drop("type")

                    schemas['mtrc'] = mtrc.schema
                    # mtrcschema = mtrc.schema


                    schemas['quarterly'] = quarterly_metric.schema
                    schemas['annual'] = annual_metric.schema
                    # srsschema = srs.schema

                    metric.append(mtrc)
                    
                    series.append(srs)

                    context.log.info(f"srsmetric_col_len:    {len(mtrc.columns)}")
                    # context.log.info(item.head())
                else:
                    mtrc = item.filter(item["type"] == "metric").drop("type")
                    
                    metric.append(mtrc)
                    context.log.info(f"metric_col_len:    {len(mtrc.columns)}")
                    # context.log.info(item.head())
                # else:
                #     context.log.info(item.head())
            else:
                print("Column 'type' does not exist in the DataFrame")

    except Exception as e:
        context.log.info(f"Pyspark Error while attempting to split dataframes: {e}")

    # Create an empty dataframe with empty schema
    mrgd_df_mtrc = spark.createDataFrame(data = [],
                           schema = schemas['mtrc'])
    mrgd_df_srs = spark.createDataFrame(data = [],
                           schema = schemas['srs'])
    try:
        for value in metric:
            mrgd_df_mtrc = mrgd_df_mtrc.unionByName(value, allowMissingColumns=True)

        for value in series:
            mrgd_df_srs = mrgd_df_srs.unionByName(value, allowMissingColumns=True)
    except Exception as e:
        context.log.info(f"Pyspark Error while attempting to merge dataframes: {e}")
    
    mrgd_df_mtrc, mrgd_df_srs = mrgd_df_mtrc.coalesce(1), mrgd_df_srs.coalesce(1)

    return mrgd_df_mtrc, mrgd_df_srs


@graph_multi_asset(
    group_name="staging",
    outs={"metric": AssetOut(metadata= {"date": dt.now().strftime("%Y-%m-%d")}, io_manager_key="s3_prqt_io_manager"),
         "series": AssetOut(metadata= {"date": dt.now().strftime("%Y-%m-%d")}, io_manager_key="s3_prqt_io_manager")
    }
)
def spark_operator(finnhub_US_stocks: List) -> tuple[DataFrame, DataFrame]:
    """
    Graph asset to map and collect transformation steps for each stock 
    into series and metric dataframes.
    """

    mapped = stock_tables(finnhub_US_stocks)
    
    collected = mapped.map(create_stock_tables)
    # metric, series = merge_and_analyze(collected.collect())
    # return metric, series
    return merge_and_analyze(collected.collect())
    