// Databricks notebook source
// MAGIC %md
// MAGIC # Databricks Delta Najlepsze praktyki optymalizacji

// COMMAND ----------

// MAGIC %fs ls mnt/training/online_retail/data-001/

// COMMAND ----------

val deltaIotPath = "/delta/iot-pipeline/"
val deltaDataPath = "/delta/customer-data/"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Problem małych plików
// MAGIC 
// MAGIC * Bardzo często dane są zapisane do małych plików, oraz mogą być rozrzucone w wielu centrach danych. 
// MAGIC * To powoduje spowolnienie ze względu na dużą ilość metadanych
// MAGIC 
// MAGIC Rozwiązaniem jest połączeniem wilelu plików w mniejszą ilość większych plików. 

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC ### OPTIMIZE
// MAGIC Databricks Delta obsługuje operację `OPTIMIZE`, która wykonuje kompaktowanie plików.
// MAGIC 
// MAGIC Najbardziej optymala wielkość pliku to około 1GB. 
// MAGIC 
// MAGIC `OPTIMIZE` nie działa automatycznie, najpierw musi zebrać małe pliki. 
// MAGIC 
// MAGIC * Przyspieszy wykonanie zapytań.
// MAGIC * Ta operacja jest kosztowna więc nie uruchamiaj jej zbyt często.

// COMMAND ----------

// MAGIC %md
// MAGIC Jeśli twoje dane nie mają dużej ilośći plików, możesz je sam stworzyć

// COMMAND ----------

display(dbutils.fs.ls(s"$deltaIotPath/date=2018-06-01/"))

// COMMAND ----------

// MAGIC %md
// MAGIC Uwaga, zapytania na tabeli składającej się z wielu plików będzie działało wolno.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM demo_iot_data_delta where deviceId=379

// COMMAND ----------

// MAGIC %md
// MAGIC ### Omijanie danych i ZORDER
// MAGIC 
// MAGIC Databricks Delta używa dwóch mechanizmów przyspieszjących zapytania (kwerendy).
// MAGIC 
// MAGIC <b>Data Skipping</b> jest uzależniona od filtra omija niepotrzebne dane (WHERE clauses). Dzięki temu mechanizmowi dane pobierane są z wyselekcjonowanej partycji 
// MAGIC 
// MAGIC 
// MAGIC <b>ZOrdering</b> jest metodą, która łączy podobne dane w te same pliki. 
// MAGIC 
// MAGIC ZOrdering mapuje wielowymarowe dane do jednego wymiaru utrzymując lokalizacje poszególnych punktów danych.
// MAGIC 
// MAGIC Przykład na kolumnie `OrderColumn` ZORDER Delta, proces
// MAGIC * bierze istniejące pliki parquet wewnątrz partycji
// MAGIC * mapuje wiersze w pliku `OrderColumn` używając algorytmu <a href="https://en.wikipedia.org/wiki/Z-order_curve" target="_blank">here</a>
// MAGIC * (jeśli jest tylko jedna kolumna, do mapowania użyte jest sortowanie liniowe(linear sort))
// MAGIC * nadpisuje posortowane dane do nowych plików parquet
// MAGIC 
// MAGIC Nie możesz użyć kolumny do ZORDER jeśli jest użyta jako kolumna partycjonowana.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### ZORDER przykład
// MAGIC Table `Students` 4 kolumny: 
// MAGIC * `gender` 2 unikalne wartośći 
// MAGIC * `Pass-Fail` 2 unikalne wartośći
// MAGIC * `Class` 4 unikalne wartośći  
// MAGIC * `Student` wiele unikalne wartośći 
// MAGIC 
// MAGIC Przykładowe zapytanie
// MAGIC 
// MAGIC ```SELECT Name FROM Students WHERE gender = 'M' AND Pass_Fail = 'P' AND Class = 'Junior'```
// MAGIC 
// MAGIC ```ORDER BY Gender, Pass_Fail```
// MAGIC 
// MAGIC Najpierw jest sortowanie po największym zbiorze `Gender`jeśli szukasz 'M' to nie trzeba patrzeć na 'F'. 
// MAGIC 
// MAGIC To zadziała jeśli dane są blisko siebie `gender = 'M'`
// MAGIC 
// MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/Delta/zorder.png" style="height: 300px"/></div><br/>

// COMMAND ----------

// MAGIC %md
// MAGIC #### ZORDER przykład
// MAGIC 
// MAGIC Syntax
// MAGIC 
// MAGIC > `OPTIMIZE Students`<br>
// MAGIC `ZORDER BY Gender, Pass_Fail`
// MAGIC 
// MAGIC To zapewni że dane `Gender = 'M' ` będą blisko siebie, to samo zostanie wykonane na kolumnie `Pass_Fail = 'P' `.
// MAGIC 
// MAGIC ZORDER, możesz posortować dane używając wielu kolumn ("col1", "col2", "col3") ale wpłynie to na utrzymanie danych blisko siebie.
// MAGIC 
// MAGIC * Jeśli używasz przetważnia strumieniowego to dane będą posortowane po czasie, wtedy do `ZORDER` użyj innej kolumny ex 'ID'.

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE demo_iot_data_delta
// MAGIC ZORDER by (deviceId)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM demo_iot_data_delta WHERE deviceId=379

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history demo_iot_data_delta

// COMMAND ----------

// MAGIC %sql
// MAGIC --select * from demo_iot_data_delta where version=2
// MAGIC 
// MAGIC SELECT count(*) FROM demo_iot_data_delta VERSION AS OF 3

// COMMAND ----------

// MAGIC %md
// MAGIC ## VACUUM
// MAGIC 
// MAGIC Żeby oszczędzić na kosztach, okazjonalnie powinninieś wyczyściś pliki 'uszkodzone' `VACUUM`. 
// MAGIC 
// MAGIC 'uszkodzone' to małe pliki powstałe przy użyciu operacji `OPTIMIZE`.
// MAGIC 
// MAGIC Syntax `VACUUM`  
// MAGIC >`VACUUM name-of-table RETAIN number-of HOURS;`
// MAGIC 
// MAGIC `number-of` ilośc godzin jako okres przechowywania.
// MAGIC 
// MAGIC Databricks nie zaleca ustawiania interwału przechowywania krótszego niż siedem dni, ponieważ stare migawki i niezatwierdzone pliki mogą być nadal używane przez procesy odczytywania lub zapisu.
// MAGIC 
// MAGIC Przykładowy scenariusz:
// MAGIC * Użytkownik A wywołuje zapytanie na nieskompaktowanych plikach
// MAGIC * Użytkownik B wywołuje `VACUUM`, ktory usunie nieskompaktowane pliki
// MAGIC * Kwerenda użytkownika A nie wykonuje się ponieważ część plików zostaje usunięta
// MAGIC 
// MAGIC Pliki mogą zostać uszkodzone podczas operacji updates/upserts/deletions.
// MAGIC 
// MAGIC Szczeguły: <a href="https://docs.databricks.com/delta/optimizations.html#garbage-collection" target="_blank"> Garbage Collection</a>.