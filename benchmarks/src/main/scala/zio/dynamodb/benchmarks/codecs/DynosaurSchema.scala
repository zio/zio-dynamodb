package zio.dynamodb.benchmarks.codecs

import cats.implicits._
import dynosaur.Schema
object DynosaurSchema {
  import BenchmarkDomain._

  implicit val mapSchema: Schema[Map[String, Int]] = Schema.dict

  implicit val tupleSchema: Schema[(Int, Long, String)] = Schema.record[(Int, Long, String)] { field =>
    (
      field("0", _._1)(Schema.int),
      field("1", _._2)(Schema.long),
      field("2", _._3)(Schema.string)
    ).mapN((a, b, c) => (a, b, c))
  }

  implicit val personSchema: Schema[Person] =
    Schema.record { field =>
      (
        field("id", _.id),
        field("name", _.name),
        field("age", _.age),
        field.opt("address", _.address),
        field("map", _.map),
        field("list", _.list),
        field("tuple", _.tuple)
      ).mapN(Person.apply)
    }

}
