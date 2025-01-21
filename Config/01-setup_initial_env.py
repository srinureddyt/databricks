# Databricks notebook source


# Pass required parameters to the notebook dependent on the environment

dbutils.widgets.text(name="env",defaultValue="",label=" Enter the environment in lower case")
env = dbutils.widgets.get("env")
print("Connected Environment :" , env)

# Mount Sorage point

storageAccountID = "'*******'"
storageAccountAccessKey = '****P'
blobContainerName = "databricks-retail-storage-bucket"
if not any(mount.mountPoint == '/mnt/unity-catalog' for mount in dbutils.fs.mounts()):
    try:
       dbutils.fs.mount(
        source="s3a://%s:%s@%s" % (storageAccountAccessKey, storageAccountID, 'databricks-retail-storage-bucket'),
        mount_point= '/mnt/unity-catalog'
  )
    except Exception as e:
        print(e)
        print("already mounted. Try to unmount first")

if not any(mount.mountPoint == '/mnt/tangirala/source-data' for mount in dbutils.fs.mounts()):
    try:
       dbutils.fs.mount(
        source="s3a://%s:%s@%s" % (storageAccountAccessKey, storageAccountID, 'tangirala/source-data'),
        mount_point= '/mnt/tangirala/source-data'
  )
    except Exception as e:
        print(e)
        print("already mounted. Try to unmount first")


if not dbutils.fs.mkdirs('/mnt/unity-catlog/bronze/'):
    print("Directory /mnt/unity-catlog/bronze/ created.")
else:
    print("Directory /mnt/unity-catlog/bronze/ already exists.")

if not dbutils.fs.mkdirs('/mnt/unity-catlog/silver/'):
    print("Directory /mnt/unity-catlog/silver/ created.")
else:
    print("Directory /mnt/unity-catlog/silver/ already exists.")

if not dbutils.fs.mkdirs('/mnt/unity-catlog/gold/'):
    print("Directory /mnt/unity-catlog/gold/ created.")
else:
    print("Directory /mnt/unity-catlog/gold/ already exists.")




# Set the database name based on the environment
def create_database(environment):
    storage_location = "s3://tangirala/unity-catalog"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{environment}_catalog` MANAGED LOCATION '{storage_location}'")
    print(f'Using {environment}_catalog ')
    print("************************************")

# Create Bronze Schema in the catalog ----------

def create_Bronze_Schema(environment,path):
    print(f'Using {environment}_Catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Bronze Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `bronze` MANAGED LOCATION '{storage_location}/bronze'""")
    print("************************************")
   
# Create Silver Schema in the catalog ----------

def create_Silver_Schema(environment,path):
    print(f'Using {environment}_Catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Silver Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `silver` MANAGED LOCATION '{storage_location}/silver'""")
    print("************************************")

# Create Gold Schema in the catalog ----------

def create_Gold_Schema(environment,path):
    print(f'Using {environment}_catalog ')
    spark.sql(f""" USE CATALOG '{environment}_catalog'""")
    print(f'Creating Gold Schema in {environment}_Catalog')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `gold` MANAGED LOCATION '/mnt/unity-catlog/gold'""")
    print("************************************")


#create_database(env)
#create_Bronze_Schema(env,'/mnt/unity-catlog/bronze/')
#create_Silver_Schema(env,'/mnt/unity-catlog/silver/')
#create_Gold_Schema(env,'/mnt/unity-catlog/gold/')
 

# COMMAND ----------

dbutils.fs.ls("/mnt/tangirala/source-data/")
