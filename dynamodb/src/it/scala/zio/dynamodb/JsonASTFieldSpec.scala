package zio.dynamodb

import zio.schema.{ DeriveSchema, Schema }
import zio.test._
import zio.test.assertTrue
import zio.ZIO
import zio.Scope
import zio.json._
import zio.json.ast.Json

object JsonASTFieldSpec extends DynamoDBLocalSpec {
  // if we uncomment this import, we do not get the directDynamicMapping coming through
  import zio.schema.codec.json._

  case class Person(id: String, json: Json)

  object Person {
    implicit val schema: Schema[Person] = DeriveSchema.gen[Person]

    val id   = ProjectionExpression.$$[Person, String]("id")
    val json = ProjectionExpression.$$[Person, Json]("json")
  }

  println(s"XXXXXXX t: ${Person.id}")
  println(s"XXXXXXX t: ${Person.json}")

  // remember DeriveSchema macro generates schema for sealed traits automatically
  sealed trait FooEnum
  object FooEnum {
    case object Bar extends FooEnum
    case object Baz extends FooEnum
  }

  case class Person2(id: String, randomEnum: FooEnum)

  object Person2 {
    implicit val schema: Schema[Person2] = DeriveSchema.gen[Person2]
  }

  // [info] person object encoded:
  // AttrMap(Map(json -> Map(ListMap(String(age) -> Number(42), String(name) -> String(John))), id -> String(id)))
  val json =
    """
      {
        "name": "John",
        "age": 42
      }
    """.stripMargin

  //[info] person object encoded:
  //AttrMap(Map(json -> Map(ListMap(String(list) -> List(Chunk(Map(ListMap(String(age) -> Number(42), String(name) -> String(John))))), String(age) -> Number(42), String(name) -> String(John))), id -> String(id)))
  val json2 =
    """
      {
        "name": "John",
        "age": 42,
        "list": [{
          "name": "John",
          "age": 42
        }]
      }
    """.stripMargin

  val json3 =
    """
      {
        "name": "John",
        "age": 42,
        "list": [1, 2, 3]
      }
    """.stripMargin

  lazy val x: Json = ???

  override def spec: Spec[Environment with Scope, Any] =
    suite("JsonASTFieldSpec")(
      test("persists json AST field as native DDB types") {
        withSingleIdKeyTable { tableName =>
          for {
            _       <- ZIO.unit
            json    <- ZIO.fromEither(json3.fromJson[Json]).mapError(e => new Exception(e))
            person   = Person("id", json)
            _       <- DynamoDBQuery.put(tableName, person).execute
            encoded <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "id")).execute
            found   <- DynamoDBQuery.get(tableName)(Person.id.partitionKey === "id").execute.absolve
            _       <- ZIO.debug(s"encoded: $encoded found: $found")
          } yield assertTrue(found == person)
        }.orDie
      }
    )


  }
