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
from pyspark.sql.functions import explode, split

# Criar sessão do Spark
spark = SparkSession.builder \
    .appName("SocketStreamWordCount") \
    .getOrCreate()

# Criar DataFrame Streaming a partir de um socket TCP (simulando um stream de dados)
socketDF = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Processamento: dividir as linhas em palavras
words = socketDF.select(explode(split(socketDF.value, " ")).alias("word"))

# Contagem de palavras
wordCounts = words.groupBy("word").count()

# Escrever a saída no console em modo streaming
query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Manter a aplicação rodando
query.awaitTermination()
