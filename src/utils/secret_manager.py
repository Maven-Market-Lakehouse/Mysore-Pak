# def get_kafka_credentials(dbutils, config):

#     scope = config["kafka"]["scope"]

#     bootstrap = dbutils.secrets.get(scope, config["kafka"]["bootstrap_key"])
#     api_key = dbutils.secrets.get(scope, config["kafka"]["api_key_name"])
#     api_secret = dbutils.secrets.get(scope, config["kafka"]["api_secret_name"])

#     return bootstrap, api_key, api_secret
    
