// Databricks notebook source
val nestedJsonDf = spark.read.format("json")
                         .option("inferSchema", "true")
                         .option("multiLine", "true")
                         .load("/FileStore/tables/Nested.json")
display(nestedJsonDf)

// COMMAND ----------

display(nestedJsonDf.withColumn("pathLinkInfo_edit", $"pathLinkInfo".dropFields("alternateName", "surfaceType")))

// COMMAND ----------

import org.apache.spark.sql.functions._

val prices: Seq[Double] = Seq(1.7, 2.9, 2.3)
val sum = prices.foldLeft(0.0)(_ + _)
print(sum)

// COMMAND ----------

case class Person(name: String, sex: String)
val persons = List(Person("Thomas", "male"), Person("Sowell", "male"), Person("Liz", "female"))

// COMMAND ----------

val foldedList = persons.foldLeft(List[String]()) { (accumulator, person) =>
  val title = person.sex match {
    case "male" => "Mr."
    case "female" => "Ms."
  }
  accumulator :+ s"$title ${person.name}"
}

assert(foldedList == List("Mr. Thomas", "Mr. Sowell", "Ms. Liz"))

// COMMAND ----------

class Foo(val name: String, val age: Int, val sex: Symbol)

object Foo {
  def apply(name: String, age: Int, sex: Symbol) = new Foo(name, age, sex)
}

val fooList = Foo("Hugh Jass", 25, 'male) ::
              Foo("Biggus Dickus", 43, 'male) ::
              Foo("Incontinentia Buttocks", 37, 'female) ::
              Nil

// COMMAND ----------

val stringList = fooList.foldLeft(List[String]()) { (z, f) =>
  val title = f.sex match {
    case 'male => "Mr."
    case 'female => "Ms."
  }
  z :+ s"$title ${f.name}, ${f.age}"
}


// COMMAND ----------

