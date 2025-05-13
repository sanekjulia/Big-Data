# Databricks notebook source
# MAGIC %md
# MAGIC #Partycjonowanie
# MAGIC
# MAGIC * Wikipedia odwiedziny strony
# MAGIC
# MAGIC * Różnice pomiędzy partycjami a slots/cores
# MAGIC * Porównanie `repartition(n)` and `coalesce(n)`
# MAGIC * Shuffle partitions
# MAGIC
# MAGIC https://dumps.wikimedia.org/other/pageviews/readme.html

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 


schema = StructType([
    StructField("domain_code", StringType(), True),
    StructField("page_title", StringType(), False),
    StructField("count_views", IntegerType(), True),
    StructField("total_response_size",IntegerType(),True)
  ])

fileName = "dbfs:/FileStore/tables/Files/pageviews/pageviews_20250101_000000-1.gz"

initialDF = (spark.read
  .option("header", "true")
  .option("sep", " ")
  .option("header","True")
  .schema(schema)
  .csv(fileName))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partycje kontra Sloty
# MAGIC
# MAGIC ** *The Spark API uses the term **core** meaning a thread available for parallel execution.*<br/>*Here we refer to it as **slot** to avoid confusion with the number of cores in the underlying CPU(s)*<br/>*to which there isn't necessarily an equal number.*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Slots/Cores
# MAGIC
# MAGIC Sprawdzam ile jest slotów `SparkContext.defaultParallelism`
# MAGIC
# MAGIC Dokumentacja <a href="https://spark.apache.org/docs/latest/configuration.html#execution-behavior" target="_blank">Spark Configuration, Execution Behavior</a>
# MAGIC
# MAGIC > Może zależeć od manager clustra:
# MAGIC > * Local mode: number of cores on the local machine
# MAGIC > * Mesos fine grained mode: 8
# MAGIC > * **Others: total number of cores on all executor nodes or 2, whichever is larger**

# COMMAND ----------

# MAGIC %md
# MAGIC Sprawdz jaki jest paralelism w spark context

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

# MAGIC %md
# MAGIC ### Partitions
# MAGIC
# MAGIC * Ile jest partycji
# MAGIC
# MAGIC Jak sprawdzić ilość partycji 
# MAGIC * wykonaj konwersję do `RDD`
# MAGIC * zapytaj o `RDD` ilość partycji 
# MAGIC

# COMMAND ----------

initialDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC Dlaczego tylko jedna partycje, czy to może przez to że wczytuję nie podzielny plik ???<br>
# MAGIC Załaduj rozpakowany plik i wczytaj jeszcze raz

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 


schema = StructType([
    StructField("domain_code", StringType(), True),
    StructField("page_title", StringType(), False),
    StructField("count_views", IntegerType(), True),
    StructField("total_response_size",IntegerType(),True)
  ])

fileName = "/FileStore/tables/Files/pageviews/pageviews_20250101_000000"

unzippedDF = (spark.read
  .option("header", "true")
  .option("sep", " ")
  .option("header","True")
  .schema(schema)
  .csv(fileName))


# COMMAND ----------

# MAGIC %md
# MAGIC Teraz lepiej, inny rodzaj pliku i od razu inna ilość partycji.

# COMMAND ----------

unzippedDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC Zapisz do innej ścieżki i podejżyj ile jest plików ???

# COMMAND ----------

unzippedDF.write.format("parquet").mode("overwrite").save("/FileStore/tables/training/wikipedia/pageviews/")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/training/wikipedia/pageviews/"))

# COMMAND ----------

initialDF.count() 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * To nie przypadek źe mam **8 slots** i **8 partitions**
# MAGIC * Spark sprawdza ile jest **slots**, i na rozmiar danych i domyślnie ustawia ilość partycji.
# MAGIC * Nawet jeśli zwiększe ilość danych Spark wczyta **8 partycji**.
# MAGIC </br>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md Wczytuję kopię danych ale już podzielonych na partycję

# COMMAND ----------

alternateDF = (spark.read
  .format("parquet").load("/FileStore/tables/training/wikipedia/pageviews/"))

print("Partycje: " + str(alternateDF.rdd.getNumPartitions()))

# COMMAND ----------

# MAGIC %md
# MAGIC **1** Co się stanie jeśli będę miał duży plik z **200 partycjami** i **256 slotów**?
# MAGIC
# MAGIC **2** Co jeśli będę miał bardzo duży plik **200 partycji** i będę miał tylko **8 slotów**, jak długo potrwa ładowanie w porównianiu z datasetem który ma tylko 8 partycji?
# MAGIC
# MAGIC **2** Jakie mam opcję jeśli mam (**200 partycji** i **8 slotów**) jeśli nie jestem w stanie zwiększyć ilośći slotów?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Użyj każdego Slot/Core
# MAGIC
# MAGIC Poza kilkoma wyjątkami staraj się dopasować ilość **partycji do ilośći slotów **.
# MAGIC
# MAGIC Dzięki temu **wszystkie sloty zostaną użyte** i każdy będzie miał przypisany **task**.
# MAGIC
# MAGIC
# MAGIC
# MAGIC Mając 5 partycji i 8 slotów **3 sloty nie będą użyte**.
# MAGIC
# MAGIC Mając 9 partycji i 8 slotów **job zajmię 2x więcej czasu**.
# MAGIC * Np może to zająć 10 sekund, żeby przetwożyć pierwszych 8  a potem kolejne 10 sekund na ostatnią partycję = 20s.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ile Partycji?
# MAGIC
# MAGIC Podstawowa wartość sugerowana to **200MB na partycję (cached)**.
# MAGIC * Nie patrz na rozmiar na dysku: CSV zajmuje dużo miejsca na dysku ale mniej w RAM: String "12345" = 10B, Integer 12345=4B.
# MAGIC * Parquet skompresowane na dysku ale nie w RAM.
# MAGIC * Relacyjne bazy i inne źródła .....?
# MAGIC
# MAGIC Wartość **200** pochodzi z doświadczeń Databricks oparty na wydajnośći. 
# MAGIC
# MAGIC Jeśli masz wykonawce o niższym RAM (np JVMs with 6GB) możesz  obniżyć tą wartość.
# MAGIC
# MAGIC Ile RAM Np 8 partycji * 200MB = 1.6GB
# MAGIC
# MAGIC
# MAGIC **Pytanie:** Jeśli moje dane będą miały 10 partycji co powinien zrobić ?...
# MAGIC * zredukować ilość partycji (1x ilość slotów)
# MAGIC * czy zwiększyć (2x ilość slotów)
# MAGIC
# MAGIC **Odpowiedź** To zależy od ilośći danych w partycji
# MAGIC * Wczytaj dane. 
# MAGIC * Cache.
# MAGIC * Sprawdź wielkość partycji.
# MAGIC * Jeśli jest powyżej > 200MB to rozważ zwiększenie ilośći partycji.
# MAGIC * Jeśli jest poniżej < 200MB to możesz zmiejszyć ilość partycji.
# MAGIC
# MAGIC **Celem jest użycie jak najmniejszej liczby partycji i utrzymanie poziomu slotów (przynajmniej 1 x partycji)**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## `coalesce()` i `repartition()`
# MAGIC
# MAGIC
# MAGIC **`coalesce(n)`** :
# MAGIC > Returns a new Dataset that has exactly numPartitions partitions, when fewer partitions are requested.<br/>
# MAGIC > If a larger number of partitions is requested, it will stay at the current number of partitions.
# MAGIC
# MAGIC **`repartition(n)`** :
# MAGIC > Returns a new Dataset that has exactly numPartitions partitions.
# MAGIC
# MAGIC Różnice
# MAGIC * `coalesce(n)` transformacja **narrow** zmiejsza ilość partycji.
# MAGIC * `repartition(n)` transformacja **wide** może być użyta do zmiejszenia lub zwiększenia ilośći partycji.
# MAGIC
# MAGIC
# MAGIC Kiedy użyć jednej lub drugiej.
# MAGIC * `coalesce(n)` nie wywoła shuffle.
# MAGIC * `coalesce(n)` nie gwarantuje równej dystrybujci rekordów na wszystkich partycjach. Może się skończyć z partycjami zawierającymi 80% danych.
# MAGIC * `repartition(n)` jako transformacja **wide** doda koszt shuffle
# MAGIC * `repartition(n)` będzie miało relatywnie równą dystrybujcę danych w partycjach.

# COMMAND ----------


repartitionedDF = alternateDF.repartition(10)

print("Partitions: " + str(repartitionedDF.rdd.getNumPartitions()))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Cache
# MAGIC
# MAGIC Back to list...
# MAGIC 0. Cache the data
# MAGIC 0. Adjust the `spark.sql.shuffle.partitions`
# MAGIC 0. Perform some basic ETL (i.e., convert strings to timestamp)
# MAGIC 0. Possibly re-cache the data if the ETL was costly
# MAGIC
# MAGIC We just balanced the number of partitions to the number of slots.
# MAGIC
# MAGIC Depending on the size of the data and the number of partitions, the shuffle operation can be fairly expensive (though necessary).
# MAGIC
# MAGIC Let's cache the result of the `repartition(n)` call..
# MAGIC * Or more specifically, let's mark it for caching.
# MAGIC * The actual cache will occur later once an action is performed
# MAGIC * Or you could just execute a count to force materialization of the cache.

# COMMAND ----------

df = repartitionedDF.cache()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##spark.sql.shuffle.partitions
# MAGIC
# MAGIC
# MAGIC 0. Adjust the `spark.sql.shuffle.partitions`
# MAGIC 0. Perform some basic ETL (i.e., convert strings to timestamp)
# MAGIC 0. Possibly re-cache the data if the ETL was costly
# MAGIC
# MAGIC The next problem has to do with a side effect of certain **wide** transformations.
# MAGIC
# MAGIC So far, we haven't hit any **wide** transformations other than `repartition(n)`
# MAGIC * But eventually we will... 
# MAGIC * Let's illustrate the problem that we will **eventually** hit
# MAGIC * We can do this by simply sorting our data.

# COMMAND ----------


(repartitionedDF
  .orderBy("page_title")        # sortuje dane 
  .rdd.foreach(lambda x: ...))  # nie robi nic poza wywołaniem joba

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",100)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Jedna akcja.
# MAGIC * Spark wykonał 3 zadania(jobs).
# MAGIC * Sprawdź plan wykonania.
# MAGIC * **Exchange rangepartitioning**
# MAGIC   

# COMMAND ----------


# Sprawdz exmplain ze wszystkimi rekordami
(repartitionedDF
  .orderBy("count_views")
  .explain())


# Sprawdz exmplain z 3M rekordami
(repartitionedDF
  .orderBy("count_views")
  .limit(3000000)
  .explain())


# COMMAND ----------

# MAGIC %md
# MAGIC Dodatkowe zadania (job) zostały wywołane ilością danych w DataFrame

# COMMAND ----------


(repartitionedDF
  .orderBy("count_views") 
  .limit(3000000)                 
  .count())        

# COMMAND ----------

# MAGIC %md
# MAGIC Only 1 job.
# MAGIC
# MAGIC Spark's Catalyst Optimizer is optimizing our jobs for us!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Kolejny Problem
# MAGIC
# MAGIC * Uruchom orginalny dataframe.
# MAGIC * Przejrzyj wszystkie zadania.
# MAGIC * Sprawdź ile jest partycji w ostatnim jobies!

# COMMAND ----------


funkyDF = (repartitionedDF
  .orderBy("count_views"))

funkyDF.rdd.foreach(lambda x: ...)

# COMMAND ----------

# MAGIC %md
# MAGIC Czy w różnych jobach jest różna ilość partycji ?

# COMMAND ----------

funkyDF.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Wartość 200 jest domyślną i opartą na doświadczeniu, pasuje do większości scenariuszy.
# MAGIC
# MAGIC Moźesz to zmienić w konfiguracji `spark.sql.shuffle.partitions`
# MAGIC

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC Zmień na 8

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC Czy jeśli zmienię różne typy operacji na datasecie to będę miał różną liczbę partycji ?
# MAGIC Ponowne wykonanie dla porównania.

# COMMAND ----------

betterDF = (repartitionedDF.orderBy("count_views","page_title")
            .groupBy("domain_code").agg(count("page_title"))
  )
                    
betterDF.rdd.foreach(lambda x: ...)

betterDF.rdd.getNumPartitions()