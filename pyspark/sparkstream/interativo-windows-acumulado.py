from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, current_timestamp

# Criar sessão do Spark
spark = SparkSession.builder \
    .appName("SocketStreamSlidingWindow") \
    .getOrCreate()

# Criar DataFrame Streaming a partir de um socket TCP
socketDF = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Adicionar uma coluna de timestamp (tempo de chegada do dado)
socketDF = socketDF.withColumn("timestamp", current_timestamp())

# Transformação: dividir as linhas em palavras
words = socketDF.select(explode(split(socketDF.value, " ")).alias("word"), "timestamp")

## Para Manter Tudo 
wordCounts = words.groupBy("word").count()
 
# Modo update mantém o estado
query = wordCounts.writeStream \
    .outputMode("update") \ 
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()


