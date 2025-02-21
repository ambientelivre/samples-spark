from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Criar sessão do Spark
spark = SparkSession.builder \
    .appName("SparkML_Interactive") \
    .getOrCreate()

# Criar um DataFrame de exemplo (tamanho da casa vs. preço)
data = [
    (50, 200000), (60, 250000), (80, 320000), 
    (100, 400000), (120, 450000), (150, 500000)
]
columns = ["size", "price"]

df = spark.createDataFrame(data, columns)

# Transformar 'size' em vetor de features (Spark ML requer vetor)
assembler = VectorAssembler(inputCols=["size"], outputCol="features")
df_transformed = assembler.transform(df).select("features", "price")

# Criar e treinar o modelo de Regressão Linear
lr = LinearRegression(featuresCol="features", labelCol="price")
model = lr.fit(df_transformed)

# Mostrar coeficiente e intercepto
print(f"Coeficiente: {model.coefficients[0]}")
print(f"Intercepto: {model.intercept}")

# Testar o modelo com um novo valor (tamanho da casa = 110m²)
test_data = spark.createDataFrame([(110,)], ["size"])
test_transformed = assembler.transform(test_data).select("features")

predictions = model.transform(test_transformed)
predictions.show()
