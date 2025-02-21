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

# Contagem de palavras por janela de tempo DESLIZANTE
wordCounts = words.groupBy(
    window(words.timestamp, "1 minute", "10 seconds"),  # Janela de 1 minuto, deslizando a cada 10s
    words.word
).count()

# Escrever a saída no console
query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()


