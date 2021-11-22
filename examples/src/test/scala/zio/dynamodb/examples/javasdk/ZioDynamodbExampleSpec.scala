package zio.dynamodb.examples.javasdk

import zio.ZIO
import zio.dynamodb.DynamoDBQuery._
import zio.dynamodb.ProjectionExpression.$
import zio.dynamodb.examples.javasdk.Payment.{ CreditCard, DebitCard, PayPal }
import zio.dynamodb.{ DynamoDBExecutor, PrimaryKey }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.{ assertCompletes, DefaultRunnableSpec }

import java.time.Instant
import scala.annotation.tailrec
import scala.util.Try

object EitherUtil {
  def forEach[A, B](list: Iterable[A])(f: A => Either[String, B]): Either[String, Iterable[B]] = {
    @tailrec
    def loop[A2, B2](xs: Iterable[A2], acc: List[B2])(f: A2 => Either[String, B2]): Either[String, Iterable[B2]] =
      xs match {
        case head :: tail =>
          f(head) match {
            case Left(e)  => Left(e)
            case Right(a) => loop(tail, a :: acc)(f)
          }
        case Nil          => Right(acc.reverse)
      }

    loop(list.toList, List.empty)(f)
  }

//  def collectAll[A, B](list: Iterable[B]): Either[String, Iterable[B]] = forEach[A, B](list)(identity)
}

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
//  } yield zio.dynamodb.foreach(listErrorOrStudent)(identity)
  } yield EitherUtil.forEach(listErrorOrStudent)(identity)

  case class Course(name: String, code: String)
  object Course {
    implicit lazy val codec: Schema[Course] = DeriveSchema.gen[Course]
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

}
