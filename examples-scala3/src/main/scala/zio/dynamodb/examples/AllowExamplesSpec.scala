package zio.dynamodb.examples

import zio.blocks.schema.CompanionOptics
import zio.blocks.schema.SchemaExpr.RelationalOperator

import java.time.Instant

object AllowExamples {
  object Json1 {

    import zio.blocks.schema.comptime.Allows
    import Allows._

    // Only JSON-representable scalars (no UUID, Char, java.time.*)
    type JsonPrimitive =
      Primitive.Boolean | Primitive.Int | Primitive.Long |
        Primitive.Double | Primitive.String | Primitive.BigDecimal |
        Primitive.BigInt | Primitive.Unit

    def toJson[A](doc: A)(using Allows[A, Record[JsonPrimitive | Self]]): String = ???

    // Only numeric types
    type Numeric = Primitive.Int | Primitive.Long | Primitive.Double | Primitive.Float |
      Primitive.BigInt | Primitive.BigDecimal

    def aggregate[A](data: A)(using Allows[A, Record[Numeric]]): Double = ???

    final case class Person(age: Int)
    final case class Person1(date: Instant)

    val x = toJson(Person(42))
    // compile error
    //val x2 = toJson(Person1(Instant.now))

  }
  object Csv1 {
    import zio.blocks.schema.Schema
    import zio.blocks.schema.comptime.Allows
    import Allows._

    // Flat record: only primitives and optional primitives allowed
    def writeCsv[A: Schema](rows: Seq[A])(using Allows[A, Record[Primitive | Optional[Primitive]]]): Unit = ???

    // RDBMS INSERT: primitives, optional primitives, or string-keyed maps (JSONB)
    def insert[A: Schema](value: A)(using Allows[A, Record[Primitive | Optional[Primitive] | Allows.Map[Primitive, Primitive]]]): String = ???

    final case class Person(age: Int)
    object Person {
      implicit val schema: Schema[Person] = Schema.derived
    }
    final case class Person2(age: Int, list: List[Int])
    object Person2 {
      implicit val schema: Schema[Person2] = Schema.derived
    }

    val x = writeCsv(Seq(Person(42)))
    // expected to not compile
    //val x2 = writeCsv(Seq(Person2(42, List(42))))
/*
[info] compiling 1 Scala source to /Users/avinder.bahra/Workspaces/avi/zio-dynamodb/examples-scala3/target/scala-3.3.3/classes ...
[error] -- Error: /Users/avinder.bahra/Workspaces/avi/zio-dynamodb/examples-scala3/src/main/scala/zio/dynamodb/examples/AllowExamplesSpec.scala:54:49
[error] 54 |    val x2 = writeCsv(Seq(Person2(42, List(42))))
[error]    |                                                 ^
[error]    |── Allows Error ────────────────────────────────────────────────────────────────
[error]    |
[error]    |  Shape violation at Person2.list
[error]    |
[error]    |    Found:    SealedTrait(List)
[error]    |    Required: Primitive | Optional[Primitive]
[error]    |
[error]    |  Hint: Type 'scala.collection.immutable.List[scala.Int]' does not match any allowed shape
[error]    |
[error]    |────────────────────────────────────────────
 */

  }
  object SchemaExprExamples {
    import zio.blocks.schema.Schema
    import zio.blocks.schema.comptime.Allows
    import zio.blocks.schema.SchemaExpr
    import Allows._

    case class Person(id: String, age: Int)
    object Person extends CompanionOptics[Person] {
      implicit val schema: Schema[Person] = Schema.derived
      val id = $(_.id)
      val age = $(_.age)
    }

    val z: SchemaExpr[Person, Boolean] = Person.id === "abc" && Person.age === 18
    // at runtime it would be evaluated to something like:
    val x =  SchemaExpr.Relational(
      SchemaExpr.Optic(???),
      SchemaExpr.Literal(???, ???),
      RelationalOperator.Equal
    )

    /*
    I want to express a restriction on ShemaExpr for shape:

     */
  }
}
