import pandas as pd
import json
from utils.general_utils import flatten_json
import requests


# Get sites data in a flatten pandas dataframe (use for nested JSONS)
def flatten_sites_metadata_to_pandas(json_data):
    flattened_sites_data = [flatten_json(site_element, 'site_') for site_element in json_data['value']]
    # Return data as pandas dataframe
    return pd.DataFrame(flattened_sites_data)


# Get sites data in a flatten pyspark dataframe (use for nested JSONS)
def flatten_sites_metadata_to_spark(spark, json_data):
    # Create the df using the JSON response
    spark_df = spark.createDataFrame(json_data['value'])

    # Flatten the DataFrame using explode when column is nested
    for column in spark_df.columns:
        if isinstance(spark_df.schema[column].dataType, (dict, list)):
            spark_df = spark_df.withColumn(column, explode(col(column)))

    # Rename columns with a prefix
    for column in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(column, f"site_{column}")

    return spark_df

# Get sites files data in a flatten pandas dataframe (use for nested JSONS)
def flatten_files_metadata_to_pandas(json_data, prefix):
    flattened_sites_data = [flatten_json(site_element, prefix) for site_element in json_data['value']]
    # Return data as pandas dataframe
    return pd.DataFrame(flattened_sites_data)

# Get sites files data in a flatten pyspark dataframe (use for nested JSONS)
def flatten_files_metadata_to_pyspark(spark, json_data, prefix):
    # Create the df using the JSON response
    spark_df = spark.read.json(spark.sparkContext.parallelize([json_data]))

    # Flatten the DataFrame using explode when column is nested
    for column in spark_df.columns:
        if isinstance(spark_df.schema[column].dataType, (dict, list)):
            # If the column is of type dict or list, explode it
            spark_df = spark_df.withColumn(column, col(column).alias(column + '_0')).selectExpr("*", f"inline_outer({column}) as {column}")

    # Rename columns with a prefix
    for column in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(column, f"{prefix}{column}")

    return spark_df


# Upload files to specific site and folder in Sharepoint
def upload_file_to_sharepoint(access_token, site_id, filename,folder_path = ''):

    # Define upload request
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{folder_path}/{filename}:/content"
    
    # Open file to write
    with open(filename, 'rb') as file:
        # Define header for request including bytes Content-Type
        headers = {
            'Authorization': access_token,
            'Content-Type': 'application/octet-stream',
        }
        # PUT request to upload file to Sharepoint
        response = requests.put(upload_url, headers=headers, data=file)
        
        if response.status_code == 201 or response.status_code == 200:
            print(f"File '{filename}' uploaded successfully.")
        else:
            print(f"Failed to upload {filename}. please check Traceback below: {response.text}")


# Get sites metadata as response
def get_sites_metadata_as_dataframe(spark,access_token, is_pyspark = True):
    if is_pyspark = True:
        return flatten_sites_metadata_to_spark(spark, get_graph_response('https://graph.microsoft.com/v1.0/sites', access_token).json())
    else:
        return flatten_sites_metadata_to_pandas(get_graph_response('https://graph.microsoft.com/v1.0/sites', access_token).json())


        # Get specific Site ID for accessing that one
        site_id = get_target_variable_with_source_variable_from_pyspark_df(df, 'site_name', 'file_system', 'site_id')
        site_name = get_target_variable_with_source_variable_from_pyspark_df(df, 'site_name', 'file_system', 'site_name')
        print(site_name)

# Get flattend data in a flattened PySpark DataFrame (use for nested JSONS)
def flatten_sublevel_metadata_to_spark(spark, json_data, key_to_convert):
    # Create the schema by inferring data types
    schema = StructType([StructField(key, StringType(), True) for key in json_data[key_to_convert][0]])

    # Create DataFrame with schema

    spark_df = spark.createDataFrame([], schema=schema)

    # Populate the df with data
    for row in json_data[key_to_convert]:
        values = []
        for key in schema.fieldNames():
            # get values to populate df
            value = row.get(key) if isinstance(row.get(key), dict) else row.get(key)
            values.append(value)

        spark_df = spark_df.union(spark.createDataFrame([tuple(values)], schema=schema))

    return spark_df


# Get specific site metadata df with site Name
def flatten_json_response_metadata_to_spark(spark, json_response):
    try:
        # Read JSON string as a df
        df = spark.read.json(spark.sparkContext.parallelize([json_response]))

        return df
    except Exception as e:
        print(e)
        return None

