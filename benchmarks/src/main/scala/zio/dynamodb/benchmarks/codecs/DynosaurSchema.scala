package zio.dynamodb.benchmarks.codecs

import cats.implicits._
import dynosaur.Schema
object DynosaurSchema {
  import BenchmarkDomain._

  implicit val mapSchema: Schema[Map[String, Int]] = Schema.dict

  implicit val personSchema: Schema[Person] =
    Schema.record { field =>
      (
        field("id", _.id),
        field("name", _.name),
        field("age", _.age),
        field.opt("address", _.address),
        field("map", _.map),
        field("list", _.list)
      ).mapN(Person.apply)
    }

}
