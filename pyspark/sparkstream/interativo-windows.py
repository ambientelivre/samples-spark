# Para Testar 
# nc -lk -p 9999
#
# Dirite no NC
# spark streaming é incrível
# pyspark é poderoso
# spark spark spark
#
#

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window

# Criar sessão do Spark
spark = SparkSession.builder \
    .appName("SocketStreamWordCountWithWindow") \
    .getOrCreate()

# Criar DataFrame Streaming a partir de um socket TCP
socketDF = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Adicionar uma coluna de timestamp (tempo de chegada do dado)
from pyspark.sql.functions import current_timestamp
socketDF = socketDF.withColumn("timestamp", current_timestamp())

# Transformação: dividir as linhas em palavras
words = socketDF.select(explode(split(socketDF.value, " ")).alias("word"), "timestamp")

# Contagem de palavras por janela de tempo (10 segundos)
wordCounts = words.groupBy(
    window(words.timestamp, "10 seconds"),  # Janela de tempo de 10 segundos
    words.word
).count()

# Escrever a saída no console em modo streaming
query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Manter a aplicação rodando
query.awaitTermination()
