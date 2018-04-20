import Encoders._
import Functions._
import mist.api._
import org.apache.spark.sql.SparkSession


object CsvReader extends MistFn[Result] {


  override def handle: Handle[Result] = {
    withArgs(
      arg[String]("pathToCsv"),
      arg[Seq[String]]("mutators")
    )
      .withMistExtras
      .onSparkSessionWithHive((pathToCsv: String,
                               mutators: Seq[String],
                               extras: MistExtras, spark: SparkSession) => {
        import extras._


        logger.info(s"read")


        createCsv(spark, pathToCsv)

        val df = readCsvToDf(spark, pathToCsv)

        val parsedMutators: List[Map[String, String]] = mutators.map(x => {
          x.split(",").map(y => y.filterNot(c => c == '{' || c == '}' || c == '"').split(":") match {
            case Array(k, v) => k.trim -> v.trim
          }).toMap
        }).toList

        logger.info(s"transform")

        val outputInformation = for {
          df <- readCsvToDf(spark, pathToCsv)
          filteredSpaces <- filterEmptyAndSpaces(df)
          filteredNulls <- filterNulls(filteredSpaces)
          converted <- convertDataType(parsedMutators, filteredNulls)
          profiled <- profilingInformation(converted)
        } yield profiled

        logger.info(s"output")
        outputInformation.get.foreach(x => logger.info(x.toString()))

        Result(outputInformation.get.map(column => {
          new Profile(column("Column").toString,
            column("Unique_values").asInstanceOf[Long],
            column("Values").asInstanceOf[Seq[(String, Long)]].map(x => new Values(x._1, x._2)))
        }))

      })
  }
}

