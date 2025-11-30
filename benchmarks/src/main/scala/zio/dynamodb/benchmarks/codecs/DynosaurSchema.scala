package zio.dynamodb.benchmarks.codecs

import cats.implicits._
import dynosaur.Schema
object DynosaurSchema {
  import BenchmarkDomain._

  implicit val personSchema: Schema[Person] =
    Schema.record { field =>
      (
        field("id", _.id),
        field("name", _.name),
        field("age", _.age),
        field("address", _.address)
      ).mapN(Person.apply)
    }

}
