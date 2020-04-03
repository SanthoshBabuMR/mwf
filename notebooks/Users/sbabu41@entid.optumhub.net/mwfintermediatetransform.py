# Databricks notebook source
storage_account_name = "mwfstore"
storage_container_name = "transformintermediate"
storage_account_access_key = "v0vKZp609wkkroud8z7wAF0v/dT81p0OfFcA5EPQBW9IkOuDPlZqJUJesYFoeDDQ0d6gocokTQl9ONaw8Y0JqA=="

file_location = "wasbs://"+storage_container_name+"@"+storage_account_name+".blob.core.windows.net/"
file_type = "csv"

spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net", storage_account_access_key)
df = spark.read.format(file_type).option("inferSchema", "false").load(file_location)