# Databricks notebook source
#Here the app registration for this workspace is given as "edm-databricks-dev"
#The applicationId, service-credential-key-name, and directory-id are stored as secrets in the keyvault and accessed through secret scope with the name "storlmsdev-scope"

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "63e7dcd4-b0e3-41f6-8cb0-0bc6cab56dbe",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="dbricks-scope",key="bricks-new-secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/c4afff3a-e02b-44fc-9d54-00c18f65a601/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://warehouse@phvdevusastorage.dfs.core.windows.net/",
  mount_point = f"/mnt/warehouse/",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/jnj')

# COMMAND ----------


