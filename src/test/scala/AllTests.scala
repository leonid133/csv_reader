

import Functions._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql._
import org.scalatest.FunSuite


class AllTests extends FunSuite with SharedSparkContext {

  override implicit def reuseContextIfPossible: Boolean = true

//  test("Test create") {
//    val spark: SparkSession = SparkSession
//      .builder()
//      .appName(sc.appName)
//      .config(sc.getConf)
//      .getOrCreate()
//
//    val csvDataSeq = Seq(Array("name", "age", "birthday", "gender"),
//              Array("  ", "xyz", "26-01-1995", "female"),
//              Array("  ", "xyz", "", "female"),
//              Array("Joe", "26", "26-01-1995", "male"),
//              Array("Homer", "26", "26-01-1995", "male"),
//              Array("Jimbo", "26", "26-01-1995", "male"),
//              Array(null, " ", "26-01-1985", "male"),
//              Array(null, "   ", "26-01-1997", "male"),
//              Array("BoJack", "30", "26-01-1995", "male"),
//              Array("Julia", "15", "26-01-1985", "female"))
//
//    createCsv(spark, "tmp/1.csv", csvDataSeq)
//
//  }

  test("Test Read a csv file from local filesystem") {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(sc.appName)
      .config(sc.getConf)
      .getOrCreate()

    val df = readCsvToDf(spark, getClass.getResource("/Sample.csv").getPath).get
    println("Sample csv")
    df.show()
    df.printSchema()

    assert(df.isInstanceOf[DataFrame])

    val firstRow = df.collect().head

    assert(firstRow.getAs[String]("name") matches "John")
    assert(firstRow.getAs[String]("age") matches "26")
    assert(firstRow.getAs[String]("birthday") matches "26-01-1995")
    assert(firstRow.getAs[String]("gender") matches "male")

    intercept[NullPointerException] {
      val rowWithNull = df.collect()(2)
      println(rowWithNull)
      rowWithNull.getAs[String]("name").isEmpty
    }

    val rowWithSpaces = df.collect()(4)
    println(rowWithSpaces)
    assert(rowWithSpaces.getAs[String]("name") matches "  ")

  }

  test("Test Remove rows where any string column is a empty string or just spaces ") {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(sc.appName)
      .config(sc.getConf)
      .getOrCreate()

    val withoutEmptyString: DataFrame = (
      for {
        df <- readCsvToDf(spark, getClass.getResource("/Sample.csv").getPath)
        filtered <- filterEmptyAndSpaces(df)
      } yield filtered
      ).get


    println("Without Empty String")
    withoutEmptyString.show()
    assert(withoutEmptyString.count() == 3)
    assert(withoutEmptyString.collect()(2).isNullAt(0))
  }

  test("Test Convert data-type and names of the columns as per user’s choice") {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(sc.appName)
      .config(sc.getConf)
      .getOrCreate()

    val args: Seq[String] = Seq(
      """{"existing_col_name" : "name", "new_col_name" : "first_name", "new_data_type" : "string"}""",
      """{"existing_col_name" : "age", "new_col_name" : "total_years", "new_data_type" : "integer"}""",
      """{"existing_col_name" : "birthday", "new_col_name" : "d_o_b", "new_data_type" : "date", "date_expression" : "dd-MM-yyyy"}"""
    )

    val params: List[Map[String, String]] = args.map(x => {
      x.split(",").map(y => y.filterNot(c => c == '{' || c == '}' || c == '"').split(":") match {
        case Array(k, v) => k.trim -> v.trim
      }).toMap
    }).toList

    val convertedDataFrame: DataFrame = (for {
      df <- readCsvToDf(spark, getClass.getResource("/Sample.csv").getPath)
      filtered <- filterEmptyAndSpaces(df)
      converted <- convertDataType(params, filtered)
    } yield converted).get

    println("Converted Data Type")
    convertedDataFrame.printSchema
    convertedDataFrame.show()
    assert(convertedDataFrame.collect().head.getAs[String]("first_name") matches "John")
    assert(convertedDataFrame.columns.length == 3)
    assert(convertedDataFrame.collect().head.getAs[Int]("total_years") == 26)
    assert(convertedDataFrame.collect()(1).getAs[Int]("total_years") == 0)
    assert(convertedDataFrame.collect()(0).getAs[java.sql.Date]("d_o_b").getTime > 791067400000L
      && convertedDataFrame.collect()(0).getAs[java.sql.Date]("d_o_b").getTime < 791079400000L)
  }


  test("Test profiling information") {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(sc.appName)
      .config(sc.getConf)
      .getOrCreate()

    val profiling: List[Map[String, Any]] = (for {
      df <- readCsvToDf(spark, getClass.getResource("/Sample.csv").getPath)
      filtered <- filterEmptyAndSpaces(df)
      profiling <- profilingInformation(filtered)
    } yield profiling).get

    profiling.foreach(println(_))

    val ageProfile = profiling.find((p: Map[String, Any]) => p("Column").asInstanceOf[String] matches "age")
    assert(ageProfile.get("Column").asInstanceOf[String] matches "age")
    assert(ageProfile.get("Unique_values").asInstanceOf[Long] == 2)
    val ageValues = ageProfile.get("Values").asInstanceOf[Seq[(Any, Long)]]
    assert(ageValues.head._2 == 2)
    assert(ageValues.head._1.toString.toInt == 26)
  }

  test("Test Exclude - nulls") {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(sc.appName)
      .config(sc.getConf)
      .getOrCreate()

    val withoutNulls = (for {
      df <- readCsvToDf(spark, getClass.getResource("/Sample.csv").getPath)
      filtered <- filterNulls(df)
    } yield filtered).get

    println("without Nulls")
    withoutNulls.show()
    assert(withoutNulls.count() == 4)
    assert(withoutNulls.collect()(3).getAs[String]("name") matches "Pete")
  }

  test("Test lookup with another table") {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(sc.appName)
      .config(sc.getConf)
      .getOrCreate()

    val args: Seq[String] = Seq(
      """{"existing_col_name" : "name", "new_col_name" : "track name", "new_data_type" : "string"}""",
      """{"existing_col_name" : "n", "new_col_name" : "number in album", "new_data_type" : "integer"}""",
      """{"existing_col_name" : "time", "new_col_name" : "time", "new_data_type" : "string"}"""
    )

    val params: List[Map[String, String]] = args.map(x => {
      x.split(",").map(y => y.filterNot(c => c == '{' || c == '}' || c == '"').split(":") match {
        case Array(k, v) => k.trim -> v.trim
      }).toMap
    }).toList

    for {
      df <- readCsvToDf(spark, getClass.getResource("/Sample2.csv").getPath)
      filteredSpaces <- filterEmptyAndSpaces(df)
      filteredNulls <- filterNulls(filteredSpaces)
      converted <- convertDataType(params, filteredNulls)
      profiling <- profilingInformation(converted)
    } yield (df.show(),
      df.printSchema(),
      filteredSpaces.show(),
      filteredNulls.show(),
      converted.show(),
      converted.printSchema(),
      profiling.foreach(println(_)))

  }


}
