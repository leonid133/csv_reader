import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object Functions {

  def createCsv(spark: SparkSession, path: String) = {
    import spark.implicits._
    val testDataSet = Seq(CsvData("name", "age", "birthday", "gender"),
      CsvData("  ", "xyz", "26-01-1995", "female"),
      CsvData("  ", "xyz", "26-01-1995", "female"),
      CsvData("Joe", "26", "26-01-1995", "male"),
      CsvData("Homer", "26", "26-01-1995", ""),
      CsvData("Jimbo", "26", "26-01-1995", null),
      CsvData(null, " ", "26-01-1985", "male"),
      CsvData(null, "   ", "26-01-1997", "male"),
      CsvData("BoJack", "30", "26-01-1995", "male"),
      CsvData("Julia", "15", "26-01-1985", "female")).toDS()
    testDataSet.show()
    testDataSet.write.csv(path)
  }

  def readCsvToDf(spark: SparkSession, path: String): Option[DataFrame] = {

    Some(spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", true)
      .option("quote", "'")
      .option("ignoreLeadingWhiteSpace", true)
      .load(path))
  }

  def filterEmptyAndSpaces(dataFrame: DataFrame): Option[DataFrame] = {
    Some(dataFrame
      .columns
      .foldLeft(dataFrame) { (memoDf, colName) =>
        val trimRow = udf((anyCol: Any) => anyCol match {
          case stringCol: String => stringCol.trim()
          case _ => "is not a string"
        })
        memoDf.withColumn("trimmed", trimRow(col(colName)))
          .filter(col("trimmed") =!= "")
          .drop("trimmed")
      })
  }

  def filterNulls(dataFrame: DataFrame): Option[DataFrame] = {
    Some(dataFrame
      .columns
      .foldLeft(dataFrame) { (memoDf, colName) => {
        memoDf
          .filter(col(colName).isNotNull)
      }
      })
  }

  def convertDataType(mutators: List[Map[String, String]], dataFrame: DataFrame): Option[DataFrame] = {
    if (mutators.isEmpty) return Some(dataFrame)
    Some(dataFrame
      .columns
      .foldLeft(dataFrame) { (memoDf, colName) =>
        val transformationMap: Option[Map[String, String]] = mutators.find((p: Map[String, String]) => {
          val value = p("existing_col_name")
          value matches colName
        })
        transformationMap.foreach(x => println(x))

        transformationMap match {
          case Some(x: Map[String, String]) if x.getOrElse("date_expression", "").nonEmpty =>
            memoDf
              .withColumn(transformationMap.get("new_col_name"), to_date(col(colName), transformationMap.get("date_expression")))
              .drop(colName)
          case Some(x: Map[String, String]) if x.getOrElse("new_col_name", "") matches colName =>
            memoDf
              .withColumn(transformationMap.get("new_col_name"), col(colName).cast(transformationMap.get("new_data_type")))
          case Some(_: Map[String, String]) =>
            memoDf
              .withColumn(transformationMap.get("new_col_name"), col(colName).cast(transformationMap.get("new_data_type")))
              .drop(colName)
          case _ => memoDf.drop(colName)
        }
      })
  }

  def profilingInformation(dataFrame: DataFrame): Option[List[Map[String, Any]]] = {
    val columns: Array[String] = dataFrame.columns

    Some(columns.map { column => {
      Map[String, Any](
        "Column" -> column,
        "Unique_values" -> dataFrame
          .agg(countDistinct(col(column))
            .alias("count_distinct"))
          .collect()
          .head
          .getAs[Long]("count_distinct"),
        "Values" -> dataFrame.groupBy(col(column))
          .agg(count(col(column)).alias("count"))
          .collect()
          .toSeq.map(row => {
          (row.getAs[String](s"${column}"),
            row.getAs[Long]("count"))
        }).toSet
      )
    }
    }.toList)
  }

}
