package zio.dynamodb.examples
import zio._
import zio.json._
import zio.dynamodb.DynamoDBQuery
import zio.schema.{ DeriveSchema, Schema }
import zio.schema.annotation.directDynamicMapping
import zio.json.ast.Json

object JsonASTFieldExample extends ZIOAppDefault {
  import zio.schema.codec.json._
  /*
  implicit val schemaJson: Schema[Json] =
    Schema.dynamicValue.transform(toJson, fromJson).annotate(directDynamicMapping())
   */

  /*
CLASS LEVEL
[info] XXXxxxxXXX schema $Lazy$
[info] XXXxxxxXXX schema Primitive(string,Chunk())
[info] XXXxxxxXXX schema CaseClass2(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample),Person2), Field(id,$Lazy$),Field(randomEnum,Enum2(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample),FooEnum))))
[info] XXXxxxxXXX schema Enum2(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample),FooEnum))
[info] XXXxxxxXXX schema CaseClass0(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample,FooEnum),Bar), )
[info] XXXxxxxXXX schema $Lazy$
[info] XXXxxxxXXX schema Primitive(string,Chunk())
NONE
[info] XXXxxxxXXX schema Primitive(string,Chunk())
[info] XXXxxxxXXX schema Dynamic(Chunk())
[info] XXXxxxxXXX schema $Lazy$
[info] XXXxxxxXXX schema Primitive(string,Chunk())
[info] XXXxxxxXXX schema CaseClass2(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample),Person2), Field(id,$Lazy$),Field(randomEnum,Enum2(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample),FooEnum))))
[info] XXXxxxxXXX schema Enum2(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample),FooEnum))
[info] XXXxxxxXXX schema CaseClass0(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample,FooEnum),Bar), )
[info] XXXxxxxXXX schema $Lazy$
[info] XXXxxxxXXX schema Primitive(string,Chunk())
FIELD
[info] XXXxxxxXXX schema Primitive(string,Chunk())
[info] XXXxxxxXXX schema Dynamic(Chunk())
[info] XXXxxxxXXX schema $Lazy$
[info] XXXxxxxXXX schema Primitive(string,Chunk())
[info] XXXxxxxXXX schema CaseClass2(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample),Person2), Field(id,$Lazy$),Field(randomEnum,Enum2(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample),FooEnum))))
[info] XXXxxxxXXX schema Enum2(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample),FooEnum))
[info] XXXxxxxXXX schema CaseClass0(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample,FooEnum),Bar), )
[info] XXXxxxxXXX schema $Lazy$
[info] XXXxxxxXXX schema Primitive(string,Chunk())
   */

  //@directDynamicMapping // only seems to be recognised when applied by json package implicit
  case class Person(id: String, json: Json)

  object Person {
    implicit val schema: Schema[Person] = DeriveSchema.gen[Person].annotate(directDynamicMapping())

    // implicit val jsonCodec: zio.json.JsonCodec[Person] =
    //   zio.schema.codec.JsonCodec.jsonCodec(schema)
  }

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

  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    for {
      _             <- ZIO.debug(s"JSON Codec Example: ${Person.schema}")
      json          <- ZIO.fromEither("""{"name":"John","age":42}""".fromJson[Json])
      person: Person = Person("id", json)
      encoded        = DynamoDBQuery.toItem(person)
      _             <- ZIO.debug(s"person object encoded: $encoded")
//JSON encoded an an ENUM/AST
//person object encoded: AttrMap(Map(json -> Map(Map(String(Obj) -> Map(Map(String(fields) -> List(Chunk(List(Chunk(String(name),Map(Map(String(Str) -> Map(Map(String(value) -> String(John))))))),List(Chunk(String(age),Map(Map(String(Num) -> Map(Map(String(value) -> Number(42))))))))))))), id -> String(id)))
//JSON encoded DIRECTLY
//[info] person object encoded: AttrMap(Map(json -> Map(ListMap(String(age) -> Number(42), String(name) -> String(John))), id -> String(id)))
      decoded       <- ZIO.fromEither(DynamoDBQuery.fromItem[Person](encoded))
      _             <- ZIO.debug(s"Item decoded to Person class: $decoded")

      _              <- ZIO.debug(s"Random enum Codec Example: ${Person2.schema}")
      person: Person2 = Person2("id", FooEnum.Bar)
      encoded         = DynamoDBQuery.toItem(person)
      _              <- ZIO.debug(s"person object encoded: $encoded")
      decoded        <- ZIO.fromEither(DynamoDBQuery.fromItem[Person2](encoded))
      _              <- ZIO.debug(s"Item decoded to Person class: $decoded")

    } yield ()
}
