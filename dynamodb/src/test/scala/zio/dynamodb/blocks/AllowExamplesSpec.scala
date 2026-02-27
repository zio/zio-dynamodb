package zio.dynamodb.blocks

import zio.test.{ assertTrue, ZIOSpecDefault }

import java.time.Instant

class AllowExamplesSpec extends ZIOSpecDefault {
  val spec =
    suite("AllowExamplesSpec")(
      test("simple example") {

        assertTrue(true)
      }
    )
}

object AllowExamples {
  object Json1 {

    import zio.blocks.schema.comptime.Allows
    import Allows._

    // Only JSON-representable scalars (no UUID, Char, java.time.*)
    type JsonPrimitive =
      Primitive.Boolean | Primitive.Int | Primitive.Long | Primitive.Double | Primitive.String | Primitive.BigDecimal | Primitive.BigInt | Primitive.Unit

    def toJson[A](doc: A)(implicit ev: Allows[A, Record[JsonPrimitive | Self]]): String = ???

    // Only numeric types
    type Numeric =
      Primitive.Int | Primitive.Long | Primitive.Double | Primitive.Float | Primitive.BigInt | Primitive.BigDecimal

    def aggregate[A](data: A)(implicit ev: Allows[A, Record[Numeric]]): Double = ???

    final case class Person(age: Int)
    final case class Person1(date: Instant)

    // val x = toJson(Person(42))
    /*
[error] /Users/avinder.bahra/Workspaces/avi/zio-dynamodb/dynamodb/src/test/scala/zio/dynamodb/blocks/AllowExamplesSpec.scala:44:19: could not find implicit value for parameter ev: zio.blocks.schema.comptime.Allows[zio.dynamodb.blocks.AllowExamples.Json1.Person,zio.blocks.schema.comptime.Allows.Record[zio.dynamodb.blocks.AllowExamples.Json1.JsonPrimitive | zio.blocks.schema.comptime.Allows.Self]]
[error]     val x = toJson(Person(42))
     */

  }
  object Csv1 {
    import zio.blocks.schema.Schema
    import zio.blocks.schema.comptime.Allows
    import Allows._

    // Flat record: only primitives and optional primitives allowed
    def writeCsv[A: Schema](rows: Seq[A])(implicit ev: Allows[A, Record[Primitive | Optional[Primitive]]]): Unit = ???

    // RDBMS INSERT: primitives, optional primitives, or string-keyed maps (JSONB)
    def insert[A: Schema](value: A)(implicit
      ev: Allows[A, Record[Primitive | Optional[Primitive] | Allows.Map[Primitive, Primitive]]]
    ): String = ???

    final case class Person(age: Int)
    object Person {
      implicit val schema: Schema[Person] = Schema.derived
    }

//    val x = writeCsv(Seq(Person(42)))

    /* In Scala 2 I get below compile error, in Scala3 it compiles OK
[error] /Users/avinder.bahra/Workspaces/avi/zio-dynamodb/dynamodb/src/test/scala/zio/dynamodb/blocks/AllowExamplesSpec.scala:63:21:
could not find implicit value for parameter ev: zio.blocks.schema.comptime.Allows[zio.dynamodb.blocks.AllowExamples.Csv1.Person,zio.blocks.schema.comptime.Allows.Record[zio.blocks.schema.comptime.Allows.Primitive | zio.blocks.schema.comptime.Allows.Optional[zio.blocks.schema.comptime.Allows.Primitive]]]
[error]     val x = writeCsv(Seq(Person(42)))
     */
  }
}
