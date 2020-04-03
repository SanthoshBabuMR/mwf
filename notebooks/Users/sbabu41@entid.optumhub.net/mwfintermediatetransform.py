# Databricks notebook source
storage_account_name = "mwfstore"
storage_container_name = "transformintermediate"
storage_account_access_key = "v0vKZp609wkkroud8z7wAF0v/dT81p0OfFcA5EPQBW9IkOuDPlZqJUJesYFoeDDQ0d6gocokTQl9ONaw8Y0JqA=="

file_location = "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/"
file_type = "csv"

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
df = spark.read.format(file_type).option("inferSchema", "false").schema(schema).load(file_location)
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
    StructField("Modifier",StringType(),True),
])

input_list = df.collect()
output_list = []

for row in input_list:
  limit = row['thru'] - row['from']
  next_val = row['from'];
  
  output_list.append((row['prefix'] + str(next_val), row['modifierUA']))
  output_list.append((row['prefix'] + str(next_val), row['modifierUB']))
  # modifiers = modifiers.union(sqlContext.createDataFrame([(row['prefix'] + str(next_val), row['UA'])], schema))
  # modifiers = modifiers.union(sqlContext.createDataFrame([(row['prefix'] + str(next_val), row['UB'])], schema))
  while limit > 0:
    limit = limit - 1
    next_val = next_val + 1
    output_list.append((row['prefix'] + str(next_val), row['modifierUA']))
    output_list.append((row['prefix'] + str(next_val), row['modifierUB']))
    # modifiers = modifiers.union(sqlContext.createDataFrame([(row['prefix'] + str(next_val), row['UA'])], schema))
    # modifiers = modifiers.union(sqlContext.createDataFrame([(row['prefix'] + str(next_val), row['UB'])], schema))

output_rdd = sc.parallelize(output_list)
output_df = sqlContext.createDataFrame(output_rdd, schema)
# output_df.printSchema()

print(output_df.count())
output_df.show(10)

  