import Functions._
import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.sql.SparkSession

case class CsvData(name: String, age: String, birthday: String, gender: String)


object CreateCsv extends MistFn[String] {

  override def handle: Handle[String] = {
    onSparkSessionWithHive((spark: SparkSession) => {
      createCsv(spark, "resources/Sample3.csv")
      "resources/Sample3.csv"
    })
  }

}
