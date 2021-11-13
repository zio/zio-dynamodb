package zio.dynamodb.examples.javasdk

import java.time.Instant

final case class Student(email: String, subject: String, enrollmentDate: Instant)
