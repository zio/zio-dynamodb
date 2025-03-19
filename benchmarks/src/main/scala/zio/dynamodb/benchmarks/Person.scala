package zio.dynamodb.benchmarks

import zio.dynamodb.ProjectionExpression
import zio.schema.{ DeriveSchema, Schema }

final case class Person(id: String, name: String)
object Person {
  val tableName                                                 = "Person"
  val idColumnName                                              = "id"  

  implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]
  val (id, name)                                                 = ProjectionExpression.accessors[Person]
}
