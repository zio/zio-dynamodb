package zio.dynamodb

import zio.dynamodb.AttrMap
import zio.dynamodb.AttributeValue
import zio.Chunk
import zio.schema.{ DeriveSchema, Schema }
import zio.test._
import zio.test.assertTrue
import zio.ZIO
import zio.Scope
import zio.json._
import zio.json.ast.Json

object JsonASTFieldSpec extends DynamoDBLocalSpec {

  final case class PersonDirect(id: String, json: Json)

  object PersonDirect {
    import zio.schema.codec.json._
    implicit val schema: Schema[PersonDirect] = DeriveSchema.gen[PersonDirect]

    val id   = ProjectionExpression.$$[PersonDirect, String]("id")
    val json = ProjectionExpression.$$[PersonDirect, Json]("json")
   }

  final case class PersonNonDirect(id: String, json: Json)
  object PersonNonDirect {
    // without this import we do not get the directDynamicMapping coming through
    // import zio.schema.codec.json._
    implicit val schema: Schema[PersonNonDirect] = DeriveSchema.gen[PersonNonDirect]

    val id   = ProjectionExpression.$$[PersonNonDirect, String]("id")
    val json = ProjectionExpression.$$[PersonNonDirect, Json]("json")
  }

  val jsonString =
    """
      {
        "name": "John"
      }
    """.stripMargin

  override def spec: Spec[Environment with Scope, Any] =
    suite("JsonASTFieldSpec")(
      test("persists json AST field as native DDB types with direct mapping") {
        withSingleIdKeyTable { tableName =>
          for {
            _       <- ZIO.unit
            json    <- ZIO.fromEither(jsonString.fromJson[Json]).mapError(e => new Exception(e))
            person   = PersonDirect("id", json)
            _       <- DynamoDBQuery.put(tableName, person).execute
            encoded <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "id")).execute
            found   <- DynamoDBQuery.get(tableName)(PersonDirect.id.partitionKey === "id").execute.absolve
          } yield assertTrue(
            found == person,
            encoded == Some(
              AttrMap(
                "id"   -> "id",
                "json" -> AttrMap("name" -> "John")
              )
            )
          )
        }
      },
      test("persists json AST field as native DDB types without direct mapping") {
        withSingleIdKeyTable { tableName =>
          for {
            _       <- ZIO.unit
            json    <- ZIO.fromEither(jsonString.fromJson[Json]).mapError(e => new Exception(e))
            person   = PersonNonDirect("id", json)
            _       <- DynamoDBQuery.put[PersonNonDirect](tableName, person).execute
            encoded <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "id")).execute
            found   <- DynamoDBQuery.get(tableName)(PersonNonDirect.id.partitionKey === "id").execute.absolve
          } yield assertTrue(
            found == person,
            encoded.get.toAttributeValue ==
              AttributeValue.Map(
                Map(
                  AttributeValue.String("id")   -> AttributeValue.String("id"),
                  AttributeValue.String("json") -> AttributeValue.Map(
                    Map(
                      AttributeValue.String("Obj") ->
                        AttributeValue.Map(
                          value = Map(
                            AttributeValue.String("fields") -> AttributeValue.List(
                              value = Chunk(
                                AttributeValue.List(value =
                                  Chunk(
                                    AttributeValue.String("name"),
                                    AttributeValue.Map(value =
                                      Map(
                                        AttributeValue.String("Str") -> AttributeValue.Map(
                                          Map(AttributeValue.String("value") -> AttributeValue.String("John"))
                                        )
                                      )
                                    )
                                  )
                                )
                              )
                            )
                          )
                        )
                    )
                  )
                )
              )
          )
        }
      }
    ) @@ TestAspect.nondeterministic

}
