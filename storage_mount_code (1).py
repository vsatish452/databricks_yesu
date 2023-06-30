# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "01224d14-8737-4e66-860c-0378eb514948",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="ADLS",key="skvkeyvalut"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/f4346ffa-d517-407a-b1c3-e0b24ab49b33/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://processinglayer@datapwc.dfs.core.windows.net/",
  mount_point = "/mnt/process/",
  extra_configs = configs)

# COMMAND ----------

client_id               = "01224d14-8737-4e66-860c-0378eb514948"
tenant_id               = "f4346ffa-d517-407a-b1c3-e0b24ab49b33"
client_secret           = "qkC8Q~weO49Qd0GSo9K5DzCPI_QIZJ0SndUz.cuV"
storage_account_name    = "datapwc"


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = f"abfss://processinglayer@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/processinglayer",
  extra_configs = configs)


# COMMAND ----------

storage_account_name

# COMMAND ----------

client_id               = "01224d14-8737-4e66-860c-0378eb514948"
tenant_id               = "f4346ffa-d517-407a-b1c3-e0b24ab49b33"
client_secret           = "qkC8Q~weO49Qd0GSo9K5DzCPI_QIZJ0SndUz.cuV"
storage_account_name    = "datapwc"


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = f"abfss://rawlayer@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/rawlayer/",
  extra_configs = configs)


# COMMAND ----------

#create rawlayer mount
client_id               = "01224d14-8737-4e66-860c-0378eb514948"
tenant_id               = "f4346ffa-d517-407a-b1c3-e0b24ab49b33"
client_secret           = "qkC8Q~weO49Qd0GSo9K5DzCPI_QIZJ0SndUz.cuV"
storage_account_name    = "datapwc"


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = f"abfss://submissionlayer@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/submissionlayer/",
  extra_configs = configs)


# COMMAND ----------

a="submissionlayer"
b ="rawlayer"
c ="processinglayer"

# COMMAND ----------

dbutils.fs.l

# COMMAND ----------

d = "SalesLT"

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datapwc/rawlayer/"+ d +"/"))

# COMMAND ----------

client_id               = "01224d14-8737-4e66-860c-0378eb514948"
tenant_id               = "f4346ffa-d517-407a-b1c3-e0b24ab49b33"
client_secret           = "qkC8Q~weO49Qd0GSo9K5DzCPI_QIZJ0SndUz.cuV"
storage_account_name    = "datapwc"


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = f"abfss://processinglayer@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/broze",
  extra_configs = configs)

# COMMAND ----------

client_id               = "01224d14-8737-4e66-860c-0378eb514948"
tenant_id               = "f4346ffa-d517-407a-b1c3-e0b24ab49b33"
client_secret           = "qkC8Q~weO49Qd0GSo9K5DzCPI_QIZJ0SndUz.cuV"
storage_account_name    = "datapwc"


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = f"abfss://processinglayer@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/silver",
  extra_configs = configs)

# COMMAND ----------

"""
container-name = 'pwc'
storage-account-name = 'blobpwcskv'

dbutils.fs.mount(
  source = "wasbs://pwc@blobpwcskv.blob.core.windows.net",
  mount_point = "/mnt/",
  extra_configs = {"fs.azure.account.key.blobpwcskv.blob.core.windows.net": "m6HR+48112FHbO3fBFymQoE7J/apr0rPcvKX4e8hO1dCmoIMpkhpMTzIuy9D+9BTMou4Lb192SfG+AStNTuupw==")})
"""

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://pwc@blobpwcskv.blob.core.windows.net",
  mount_point = "/mnt/",
  extra_configs = {"fs.azure.account.key.blobpwcskv.blob.core.windows.net":"m6HR+48112FHbO3fBFymQoE7J/apr0rPcvKX4e8hO1dCmoIMpkhpMTzIuy9D+9BTMou4Lb192SfG+AStNTuupw==")})

# COMMAND ----------

dbutils.fs.mount(
  source="wasbs://pwc@blobpwcskv.blob.core.windows.net",
  mount_point="/mnt/",
  extra_configs={"fs.azure.account.key.blobpwcskv.blob.core.windows.net": "m6HR+48112FHbO3fBFymQoE7J/apr0rPcvKX4e8hO1dCmoIMpkhpMTzIuy9D+9BTMou4Lb192SfG+AStNTuupw=="}
)


# COMMAND ----------


