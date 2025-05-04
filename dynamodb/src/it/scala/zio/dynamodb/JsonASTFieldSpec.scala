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

  case class PersonDirect(id: String, json: Json)

  object PersonDirect {
    // if we uncomment this import, we do not get the directDynamicMapping coming through
    import zio.schema.codec.json._
    implicit val schema: Schema[PersonDirect] = DeriveSchema.gen[PersonDirect]

    val id   = ProjectionExpression.$$[PersonDirect, String]("id")
    val json = ProjectionExpression.$$[PersonDirect, Json]("json")
  }

  case class PersonNonDirect(id: String, json: Json)
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
        "name": "John",
        "age": 42,
        "list": [1, 2, 3]
      }
    """.stripMargin

  val jsonString2 =
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
                "json" -> AttrMap("age" -> 42, "name" -> "John", "list" -> List(1, 2, 3))
              )
            )
          )
        }
      },
      test("persists json AST field as native DDB types without direct mapping") {
        withSingleIdKeyTable { tableName =>
          for {
            _       <- ZIO.unit
            json    <- ZIO.fromEither(jsonString2.fromJson[Json]).mapError(e => new Exception(e))
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
//            encoded == Some(AttrMap("id" -> "id", "json" -> AttrMap("Obj" -> AttrMap("fields" -> List(List("name"), AttrMap("Str" -> AttrMap("value" -> "John"))))))),
//            encoded == Some(AttrMap("id" -> List("1", "2")))
          )
        }
      }
    ) @@ TestAspect.nondeterministic

}
/*

    ✗ Some(AttrMap(
        map = Map(
          "json" -> Map(
            value = Map(
              String(value = "Obj") -> Map(
                value = Map(
                  String(value = "fields") -> List(
                    value = Chunk(List(
                      value = Chunk(String(value = "name"), Map(
                        value = Map(
                          String(value = "Str") -> Map(
                            value = Map(
                              String(value = "value") -> String(value = "John")
                            )
                          )
                        )
                      ))
                    ))
                  )
                )
              )
            )
          ),
          "id" -> String(value = "id")
        )
      )) was not equal to Some(AttrMap(
        map = Map(
          "id" -> String(value = "id"),
          "json" -> Map(
            value = Map(
              String(value = "Obj") -> Map(
                value = Map(
                  String(value = "fields") -> List(
                    value = Chunk(Map(
                      value = Map(
                        String(value = "Str") -> Map(
                          value = Map(
                            String(value = "value") -> String(value = "John")
                          )
                        )
                      )
                    ))
                  )
                )
              )
            )
          )
        )
      ))

 */
