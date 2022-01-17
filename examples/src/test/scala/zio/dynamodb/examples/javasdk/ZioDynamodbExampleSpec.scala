package zio.dynamodb.examples.javasdk

import zio.ZIO
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.examples.javasdk.Payment.{ CreditCard, DebitCard, PayPal }
import zio.dynamodb.{ DynamoDBExecutor, EitherUtil, PrimaryKey }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.{ assertCompletes, DefaultRunnableSpec }

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
    _                  <- (put("student", avi) zip
                              put("student", adam)).execute

    listErrorOrStudent <- forEach(List(avi, adam)) { st =>
                            get[Student](
                              "student",
                              PrimaryKey("email" -> st.email, "subject" -> st.subject)
                            )
                          }.execute

    _                  <- (updateItem("student", pk(avi))($("payment").set(PayPal.toString)) zip
                              updateItem("student", pk(adam))($("payment").set(PayPal.toString))).execute
  } yield EitherUtil.collectAll(listErrorOrStudent)

  case class Course(name: String, code: String)
  object Course {
    implicit lazy val schema: Schema[Course] = DeriveSchema.gen[Course]
  }

  val program2 = for {
    enrollmentDate <- ZIO.fromEither(parseInstant("2021-03-20T01:39:33Z"))
    avi             = Student("avi@gmail.com", "maths", Some(enrollmentDate), DebitCard)
    adam            = Student("adam@gmail.com", "english", Some(enrollmentDate), CreditCard)
    french          = Course("french", "123")
    art             = Course("art", "123")
    _              <- (put("student", avi) zip
                          put("student", adam) zip
                          put("course", french) zip
                          put("course", art) zip
                          updateItem("student", pk(avi))($("payment").set(100.0)) zip
                          updateItem("student", pk(adam))($("payment").set(PayPal.toString))).execute

    _              <- (get[Student]("student", pk(avi)) zip
                          get[Student]("student", pk(adam))).execute
  } yield ()

  val avi  = Student("avi@gmail.com", "maths", None, DebitCard)
  val adam = Student("adam@gmail.com", "english", None, CreditCard)

  val errorOrListOfStudent = (for {
    _                    <- (put("student", avi) zip put("student", adam)).execute
    listOfErrorOrStudent <- forEach(List(avi, adam)) { st =>
                              get[Student](
                                "student",
                                PrimaryKey("email" -> st.email)
                              )
                            }.execute
  } yield EitherUtil.collectAll(listOfErrorOrStudent)).provideCustomLayer(DynamoDBExecutor.test)

}
