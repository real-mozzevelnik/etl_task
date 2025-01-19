# Пример Spark Задания для таблиц ads, sales

from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--table_name', required=True)
parser.add_argument('--s3_url', required=True)
parser.add_argument('--last_update_time', required=True)
args = parser.parse_args()

spark = SparkSession.builder \
    .appName("ETL") \
    .getOrCreate()


# Система 1 и 2 не оговорены по условию задания, для примера используем постгрес
source_data = spark.read.format("jdbc").options(
    url="jdbc:postgresql://host:port/db",
    driver="org.postgresql.Driver",
    query=f"SELECT * FROM {args.table_name} WHERE date > '{args.last_update_time}'",
    user="user",
    password="password"
).load()

# Валидация и Трансформация данных

source_data.write.parquet(args.s3_url, mode='overwrite')

spark.stop()
