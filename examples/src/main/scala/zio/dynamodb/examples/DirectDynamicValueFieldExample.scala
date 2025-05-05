package zio.dynamodb.examples
import zio._
import zio.dynamodb.DynamoDBQuery
import zio.schema.{ DeriveSchema, DynamicValue, Schema, StandardType, TypeId }
import scala.collection.immutable.ListMap

/*

|-----------|-----------------------------|------------------------------|
| Dynamo AV | Schema                      | Notes                        |
|-----------|-----------------------------|------------------------------|
| Binary    | StandardType.BinaryType     |                              |
| BinarySet | X                           | DV sets are not homogeneous  |
| Bool      | StandardType.BoolType       |                              |
| List      | DynamicValue.Sequence       |                              |
| Map       | DynamicValue.Record         |                              |
| Null      | DynamicValue.NoneValue      |                              |
| Number    | StandardType.BigDecimalType |                              |
| NumberSet | X                           | DV sets are not homogeneous  |
| String    | StandardType.StringType     |                              |
| StringSet | X                           | DV sets are not homogeneous  |

 */
object DirectDynamicValueFieldExample extends ZIOAppDefault {

  import zio.schema.annotation.directDynamicMapping

  case class Person(id: String, @directDynamicMapping dv: DynamicValue)

  object Person {
    implicit val schema: Schema[Person] = DeriveSchema.gen[Person]

  }

  /*
  So field has annotation but not schema
  [info] f.annotations: Chunk(directDynamicMapping())
  [info] f.schema.annotations: Chunk()
   */
  override def run: ZIO[Environment with ZIOAppArgs with Scope, Any, Any] =
    for {
      _             <- ZIO.debug(s"DynamicValue Codec Example Person.schema: ${Person.schema}")
      _              = Person.schema match {
                         case s: Schema.Record[_] =>
                           println(s"s.annotations: ${s.annotations}")
                           s.fields.foreach { f =>
                             println(s"***** field name ***** ${f.name}")
                             println(s"f.schema: ${f.schema}")
                             println(s"f.annotations: ${f.annotations}")
                             println(s"f.schema.annotations: ${f.schema.annotations}")
                           }
                         case _                   =>
                           println("Person.schema is not a Record")
                       }
      dynamicNum10   = DynamicValue
                         .Primitive[java.math.BigDecimal](new java.math.BigDecimal(10), StandardType.BigDecimalType)
      dynamicNum42   = DynamicValue
                         .Primitive[java.math.BigDecimal](new java.math.BigDecimal(42), StandardType.BigDecimalType)
      dv             = DynamicValue.Record(
                         id = zio.schema.TypeId.parse("zio.dynamodb.examples.JsonASTFieldExample2.PersonX"),
                         values = ListMap(
                           "name" -> DynamicValue.Primitive[String]("John", StandardType.StringType),
                           "age"  -> dynamicNum42,
                           "NS"   -> DynamicValue.Sequence(Chunk(dynamicNum10, dynamicNum42))
                         )
                       )
      person: Person = Person("id", dv)
      encoded        = DynamoDBQuery.toItem(person)
      _             <- ZIO.debug(s"person object encoded: $encoded")
      decoded       <- ZIO.fromEither(DynamoDBQuery.fromItem[Person](encoded))
      _             <- ZIO.debug(s"Item decoded to Person class: $decoded")
      _              = println(
                         s"YYYYYY printDynamicRecord(decoded.dv)._2 == printDynamicRecord(person.dv)._2 : ${extractDynamicRecord(
                           decoded.dv
                         )._2 == extractDynamicRecord(person.dv)._2}"
                       )
    } yield ()

  def extractDynamicRecord(dv: DynamicValue): (TypeId, ListMap[String, DynamicValue]) =
    dv match {
      case DynamicValue.Record(id, values) => (id, values)
      case _                               => (null, ListMap.empty)
    }
}
/*
YYYYYY  person.dv: Record(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample2),PersonX),ListMap(name -> Primitive(John,string), age -> Primitive(42,int)))
YYYYYY decoded.dv: Record(Nominal(Chunk(),Chunk(),AttributeValue.Map),                              ListMap(String(age) -> Primitive(42,bigDecimal), String(name) -> Primitive(John,string)))
WITH annotation
[info] person object encoded: AttrMap(Map(dv -> Map(Map(String(Record) -> Map(Map(String(values) -> List(Chunk(List(Chunk(String(name),Map(Map(String(String) -> String(John))))),List(Chunk(String(age),Map(Map(String(BigDecimal) -> Number(42))))),List(Chunk(String(NS),Map(Map(String(SetValue) -> Map(Map(String(values) -> List(Chunk(Map(Map(String(BigDecimal) -> Number(10))),Map(Map(String(BigDecimal) -> Number(42))))))))))))), String(id) -> String(zio.dynamodb.examples.JsonASTFieldExample2.PersonX))))), id -> String(id)))
[info] Item decoded to Person class: Person(id,Record(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample2),PersonX),ListMap(name -> Primitive(John,string), age -> Primitive(42,bigDecimal), NS -> SetValue(Set(Primitive(10,bigDecimal), Primitive(42,bigDecimal))))))
WITHOUT annotation
[info] person object encoded: AttrMap(Map(dv -> Map(Map(String(Record) -> Map(Map(String(values) -> List(Chunk(List(Chunk(String(name),Map(Map(String(String) -> String(John))))),List(Chunk(String(age),Map(Map(String(BigDecimal) -> Number(42))))),List(Chunk(String(NS),Map(Map(String(SetValue) -> Map(Map(String(values) -> List(Chunk(Map(Map(String(BigDecimal) -> Number(10))),Map(Map(String(BigDecimal) -> Number(42))))))))))))), String(id) -> String(zio.dynamodb.examples.JsonASTFieldExample2.PersonX))))), id -> String(id)))
[info] Item decoded to Person class: Person(id,Record(Nominal(Chunk(zio,dynamodb,examples),Chunk(JsonASTFieldExample2),PersonX),ListMap(name -> Primitive(John,string), age -> Primitive(42,bigDecimal), NS -> SetValue(Set(Primitive(10,bigDecimal), Primitive(42,bigDecimal))))))
 */
