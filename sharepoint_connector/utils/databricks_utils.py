# Functions to support connector with databricks
    
# Get secrets from Databricks

# Extract config variables from Databricks
def get_secret_variables():
    try:
        # Get secret variables from Databricks
        client_secret = dbutils.secrets.get(scope = 'scope',key = 'client_secret')
        client_id = dbutils.secrets.get(scope = 'scope',key = 'client_id')
        authority = dbutils.secrets.get(scope = 'scope',key = 'authority')
        scope = dbutils.secrets.get(scope = 'scope',key = 'scope')
        return client_secret, client_id,  authority, scope
    except Exception as e:
        print('''
        Databricks secrets were not fetched correctly. please make sure the the variables names are:
        - Client Secret = "client_secret"
        - Client ID = "client_id"
        - Authority = "authority"
        - Scope = "scope"

        For more information, check traceback below:
        ''')
        print(e)
    
