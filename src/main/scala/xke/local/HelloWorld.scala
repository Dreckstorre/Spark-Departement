package xke.local

import org.apache.spark.sql.{SaveMode, SparkSession}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()

    // 1) lire le fichier
    // 2) garder les codes régions pairs
    // 3) regrouper les régions par dizaine de code région
    // 4) garde que les lignes avec résultats de régions dans la dizaine
    // 5) Afficher le nombre de lignes finales

    val df = spark.read.option("delimiter",",").option("header", false).csv("src/main/resources/departements-france.csv")
    df.filter(df("code_departement") % 2 === 0).show
    df.groupBy(df("code_departement") / 10)

    df.show()
  }
}
