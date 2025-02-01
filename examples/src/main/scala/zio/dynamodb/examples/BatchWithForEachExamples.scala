package zio.dynamodb.examples

import zio.schema.Schema
import zio.schema.DeriveSchema
import zio.dynamodb.ProjectionExpression
import zio.dynamodb.DynamoDBQuery
//import zio.dynamodb.DynamoDBQuery.HasNoCondition

object BatchWithForEachExamples {
  final case class Person(id: Int, name: String)
  object Person {
    implicit val schema: Schema.CaseClass2[Int, String, Person] = DeriveSchema.gen[Person]
    val (id, name)                                              = ProjectionExpression.accessors[Person]
  }

  /*
  Problem is interaction between DynamoDBQuery and HasNoCondition is not equivalent
   */
  val query = DynamoDBQuery.forEach2(List(1, 2)) { i =>
    DynamoDBQuery.put("table", Person(i, s"Person$i"))
  }
}
