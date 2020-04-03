# Databricks notebook source
# Blob Storage Strucutre
# mwfstore                                                        (storage account)
#   input                                                           (storage container)
#     rates_input.csv                                                 (input file)
#     modifiers/30181_modifier_uaub (unmodified).csv                  (input file)
#   output                                                          (storage container )
#   intermittentoutput                                              (storage container)

# COMMAND ----------

# Configure
storage_account_name = "mwfstore"
storage_account_access_key = "v0vKZp609wkkroud8z7wAF0v/dT81p0OfFcA5EPQBW9IkOuDPlZqJUJesYFoeDDQ0d6gocokTQl9ONaw8Y0JqA=="

src_container_name = "input"
intermittentoutput_container_name = "intermittentoutput"

file_location = "wasbs://"+src_container_name+"@"+storage_account_name+".blob.core.windows.net/"
file_type = "csv"

intermittentoutput_folder = "wasbs://"+intermittentoutput_container_name+"@"+storage_account_name+".blob.core.windows.net/"
output_blob_folder = "%s/temp" % intermittentoutput_folder

spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net", storage_account_access_key)


# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
schema = StructType([
    StructField("fromCode",StringType(),False),
    StructField("thruCode",StringType(),False),
    StructField("modifierUA",StringType(),True),
    StructField("modifierUB",StringType(),True)
])
modifiers_file_location = "%s/modifiers" % file_location
df = spark.read.format(file_type).option("inferSchema", "false").schema(schema).load(modifiers_file_location)
df.show(6)

df = df.filter("thruCode != 'null' and thruCode != 'Thru'")
df.show(2)

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, regexp_replace
from pyspark.sql.types import IntegerType

extract_char = r'^([A-Z])*'
extract_num = '^([A-Z])*(\d*)'

df = df.withColumn("prefix", regexp_extract(df["fromCode"], extract_start_chars, 1))
df = df.withColumn("from", regexp_extract(df["fromCode"], extract_num, 2).cast(IntegerType()))
df = df.withColumn("thru", regexp_extract(df["thruCode"], extract_num, 2).cast(IntegerType()))
df = df.drop('fromCode').drop('thruCode')

df.show(2)

# COMMAND ----------

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
schema = StructType([
    StructField("Code",StringType(),False),
    StructField("Modifier",StringType(),False),
    StructField("Amount",StringType(),True)
])

input_list = df.collect()
output_list = []

for row in input_list:
  limit = row['thru'] - row['from']
  next_val = row['from'];
  
  output_list.append((row['prefix'] + str(next_val), "UA", row['modifierUA']))
  output_list.append((row['prefix'] + str(next_val), "UB", row['modifierUB']))
  while limit > 0:
    limit = limit - 1
    next_val = next_val + 1
    output_list.append((row['prefix'] + str(next_val), "UA", row['modifierUA']))
    output_list.append((row['prefix'] + str(next_val), "UB", row['modifierUB']))
  
output_rdd = sc.parallelize(output_list)
output_df = sqlContext.createDataFrame(output_rdd, schema)
# output_df.printSchema()

print(output_df.count())
output_df.show(10)

  

# COMMAND ----------



# write the dataframe as a single file to blob storage
(output_df
 .coalesce(1)
 .write
 .mode("overwrite")
 .option("header", "true")
 .format("com.databricks.spark.csv")
 .save(output_blob_folder))

# Get the name of the wrangled-data CSV file that was just saved to Azure blob storage (it starts with 'part-')
files = dbutils.fs.ls(output_blob_folder)
output_file = [x for x in files if x.name.startswith("part-")]

# Move the wrangled-data CSV file from a sub-folder (wrangled_data_folder) to the root of the blob container
# While simultaneously changing the file name
dbutils.fs.mv(output_file[0].path, "%s/30181_modifier_uaub (transformed).csv" % intermittentoutput_folder)

## Clean up temp files
files = dbutils.fs.ls(output_blob_folder)
for file in files:
  dbutils.fs.rm(file.path)
dbutils.fs.rm("%s/temp" % intermittentoutput_folder)  

# COMMAND ----------

# Run All Above