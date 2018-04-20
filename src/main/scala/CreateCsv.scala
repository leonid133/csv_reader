import Functions._
import mist.api._
import mist.api.args.ArgDef
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.sql.SparkSession

case class CsvData(fields: Seq[Any])


object CreateCsv extends MistFn[String] {

  override def handle: Handle[String] = {
    withArgs(
      allArgs: ArgDef[Map[String, Any]]
    )
      .withMistExtras
      .onSparkSessionWithHive((parameters: Map[String, Any],
                               extras: MistExtras,
                               spark: SparkSession) => {
        import extras._

        logger.info("parameters")
        parameters.foreach(x => {
          logger.info({x._1 + "->" + x._2.toString})
        })


        val pathToCsv: String = parameters("path").asInstanceOf[String]

        val csvDataSeq: Seq[(Int, Array[String])] = parameters
          .filter(x => x._1 != "path")
          .map(x => {
            (x._1.asInstanceOf[String].toInt, x._2
            .asInstanceOf[String]
            .split(",")
            .map(x => {
              if (x contains "null") {
                null
              } else x
            }))
          }).toSeq

        val sortedCsvDataSeq: Seq[Array[String]] = csvDataSeq
            .sortWith(_._1 < _._1)
            .map(x => x._2)

        logger.info("csvDataSeq")
        sortedCsvDataSeq.foreach(x => {
          logger.info("new line")
          x.foreach(y => {
            logger.info(y)
          })
        })

        createCsv(spark, pathToCsv, sortedCsvDataSeq)
        pathToCsv
      })
  }

}
