import mist.api.encoding.Encoder

case class Result(res: List[Profile])

case class Profile(column: String, uniqueValues: Long, values: Seq[Values])

case class Values(value: String, count: Long)

object Encoders {

  import mist.api.data._

  implicit def valuesEncoder: Encoder[Values] = new Encoder[Values] {
    override def apply(a: Values): JsLikeData = JsLikeMap(
      a.value -> JsLikeNumber(a.count)
    )
  }

  implicit def profileEncoder: Encoder[Profile] = new Encoder[Profile] {
    override def apply(a: Profile): JsLikeData = JsLikeMap(
      "Column" -> JsLikeString(a.column),
      "Unique_values" -> JsLikeNumber(a.uniqueValues),
      "Values" -> JsLikeList(a.values.map(v => valuesEncoder(v)))
    )
  }

  implicit def resultEncoder: Encoder[Result] = new Encoder[Result] {
    override def apply(a: Result): JsLikeData = JsLikeList(a.res.map(x => profileEncoder(x)))
  }
}