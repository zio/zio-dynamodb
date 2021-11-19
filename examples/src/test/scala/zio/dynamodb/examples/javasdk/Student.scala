package zio.dynamodb.examples.javasdk

import zio.schema.{ DefaultJavaTimeSchemas, DeriveSchema, Schema }

import java.time.Instant

final case class Student(email: String, subject: String, enrollmentDate: Option[Instant], payment: Payment)
object Student extends DefaultJavaTimeSchemas {
  implicit lazy val codec: Schema[Student] = DeriveSchema.gen[Student]
}
