# Flatten JSON
def flatten_json(json_input, prefix=''):

    # Create empty dictionary to collect JSON data
    flattened_json_dictionary = {}
    
    # Get key:value items from JSON
    for key, value in json_input.items():
        # Recursively flatten nested dictionaries within JSON
        if isinstance(value, dict):
            flattened_json_dictionary.update(flatten_json(value, f"{prefix}{key}_"))
        else:
            # Update the flattened dictionary with the current key:value item
            flattened_json_dictionary[f"{prefix}{key}"] = value

    return flattened_json_dictionary

# Get Variables within the same row
def get_targetVariable_with_sourceVariable_from_pandas_df(df, source_column, source_value, target_column_to_fetch_value):
    # This function extract the specific value from a column given the input of a different column and it's reference value
    #  Example sites dataframe include multiple data in its metadata, but only the id is necessary in this case,
    #  therefore to get the site's ID we look for the id using the site name of interest 
    #  E.g. get_targetVariable_with_sourceVariable_from_df(df, 'site_name', 'file_system', 'site_id')
    try:
        result = df.loc[df[source_column] == source_value, target_column_to_fetch_value].iloc[0]
        return result
    except Exception as e:
        print(f'Value not founded, please check the input variables. Check the traceback below:{e}')
        return None


def get_target_variable_with_source_variable_from_pyspark_df(spark, df, source_column, source_value, target_column_to_fetch_value):
    # Same as above but for pyspark
    try:
        result = df.filter(col(source_column) == source_value).select(target_column_to_fetch_value).first()[0]
        return result
    except Exception as e:
        print(f'Value not found, please check the input variables. Check the traceback below:{e}')
        return None   


