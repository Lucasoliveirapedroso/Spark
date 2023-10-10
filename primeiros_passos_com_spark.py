# Importa a classe SparkSession do módulo pyspark.sql
from pyspark.sql import SparkSession

# Cria uma instância do SparkSession com um nome de aplicativo e configuração
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL exemplo básico") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Lê um arquivo JSON em um DataFrame chamado "df"
df = spark.read.json("C:/Users/Lucas/OneDrive/Documentos/Spark/people.json")

# Exibe as primeiras linhas do DataFrame
df.show() 

'''
Resultado
+----+-------+
| age|   name|
+----+-------+
|NULL|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
'''

df.printSchema() # Exibe o esquema do DataFrame
'''
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)
'''
df.select("name").show() # Seleciona a coluna "name" do DataFrame
'''
+-------+
|   name|
+-------+
|Michael|
|   Andy|
| Justin|
+-------+
'''

df.select(df['name'], df['age'] + 1).show() # Seleciona as colunas "name" e "age + 1" do DataFrame
'''
+-------+---------+
|   name|(age + 1)|
+-------+---------+
|Michael|     NULL|
|   Andy|       31|
| Justin|       20|
+-------+---------+
'''
df.groupBy("age").count().show() # Agrupa o DataFrame por idade e conta o número de ocorrências em cada grupo
'''
+----+-----+
| age|count|
+----+-----+
|  19|    1|
|NULL|    1|
|  30|    1|
+----+-----+
'''

# Cria ou substitui uma exibição temporária chamada "people"
df.createOrReplaceTempView("people")

#Executa uma consulta SQL no DataFrame usando a exibição temporária
sqlDF = spark.sql("SELECT * FROM people") 
sqlDF.show()
'''
+----+-------+
| age|   name|
+----+-------+
|NULL|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+

'''

# Cria uma exibição temporária global chamada "people"
df.createGlobalTempView("people")
'''
+----+-------+
| age|   name|
+----+-------+
|NULL|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
'''

# Executa uma consulta SQL no DataFrame usando a exibição temporária global
spark.sql("SELECT * FROM global_temp.people").show()

'''
+----+-------+
| age|   name|
+----+-------+
|NULL|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
'''

# Cria uma nova sessão Spark e executa uma consulta SQL na exibição temporária global
spark.newSession().sql("SELECT * FROM global_temp.people").show()

'''
+----+-------+
| age|   name|
+----+-------+
|NULL|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
'''