package zio.dynamodb.examples.javasdk

import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.examples.javasdk.Payment.{ CreditCard, DebitCard, PayPal }
import zio.dynamodb.{ DynamoDBExecutor, DynamoDBQuery, PrimaryKey }
import zio.test.{ assertCompletes, DefaultRunnableSpec }
import zio.{ Has, ZIO }

import java.time.Instant
import scala.util.Try

object ZioDynamodbExampleSpec extends DefaultRunnableSpec {
  override def spec =
    suite("zio-dynamodb")(test("test1") {
      assertCompletes
    }).provideCustomLayer(DynamoDBExecutor.test)

  def parseInstant(s: String): Either[String, Instant] = Try(Instant.parse(s)).toEither.left.map(_.getMessage)
  def pk(st: Student): PrimaryKey                      = PrimaryKey("email" -> st.email, "subject" -> st.subject)

  val program = for {
    enrollmentDate     <- ZIO.fromEither(parseInstant("2021-03-20T01:39:33Z"))
    avi                 = Student("avi@gmail.com", "maths", Some(enrollmentDate), DebitCard)
    adam                = Student("adam@gmail.com", "english", Some(enrollmentDate), CreditCard)
    _                  <- (DynamoDBQuery.put("student", avi) zip
                              DynamoDBQuery.put("student", adam)).execute

    listErrorOrStudent <- DynamoDBQuery
                            .forEach(List(avi, adam)) { st =>
                              DynamoDBQuery.get[Student](
                                "student",
                                PrimaryKey("email" -> st.email, "subject" -> st.subject)
                              )
                            }
                            .execute

    _                  <- (DynamoDBQuery
                              .updateItem("student", pk(avi))($("payment").set(PayPal.toString)) zip
                              DynamoDBQuery
                                .updateItem("student", pk(adam))($("payment").set(PayPal.toString))).execute
  } yield zio.dynamodb.foreach(listErrorOrStudent)(identity)

  val program2 = for {
    enrollmentDate     <- ZIO.fromEither(parseInstant("2021-03-20T01:39:33Z"))
    avi                 = Student("avi@gmail.com", "maths", Some(enrollmentDate), DebitCard)
    adam                = Student("adam@gmail.com", "english", Some(enrollmentDate), CreditCard)
    _                  <- (DynamoDBQuery.put("student", avi) zip
                              DynamoDBQuery.put("student", adam)).execute

    _                  <- (DynamoDBQuery.get[Student]("student", pk(avi)) zip
                              DynamoDBQuery.get[Student]("student", pk(adam))).execute
    listErrorOrStudent <- DynamoDBQuery
                            .forEach(List(avi, adam)) { st =>
                              DynamoDBQuery.get[Student](
                                "student",
                                PrimaryKey("email" -> st.email, "subject" -> st.subject)
                              )
                            }
                            .execute

    _                  <- (DynamoDBQuery
                              .updateItem("student", pk(avi))($("payment").set(PayPal.toString)) zip
                              DynamoDBQuery
                                .updateItem("student", pk(adam))($("payment").set(PayPal.toString))).execute
  } yield zio.dynamodb.foreach(listErrorOrStudent)(identity)

}
