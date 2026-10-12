package zio.dynamodb

import cats.data.EitherT
import cats.effect.{ IO => CEIO }
import cats.effect.unsafe.implicits.global
import software.amazon.awssdk.awscore.exception.AwsServiceException
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import zio._
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.DynamoDBError.CancellationCode
import zio.dynamodb.DynamoDBError.ItemError.{ ConditionFailed, DecodingError }
import zio.dynamodb.DynamoDBError.TransactionError.TransactionCancelled
import zio.dynamodb.ExecuteSyntax._
import zio.dynamodb.blocks.ddbexpr.DdbExprApi
import zio.dynamodb.blocks.ddbexpr.dsl._
import zio.test._

// THROWAWAY: examples, not tests. Each handles the Either result properly and prints the outcome.
object WhereAndTxOutcomesExamples extends DynamoDBLocalSpec {

  final case class Score(id: String, score: Int)
  object Score extends CompanionOptics[Score] {
    implicit val schema: Schema[Score] = Schema.derived
    val id: Lens[Score, String]        = $(_.id)
    val score: Lens[Score, Int]        = $(_.score)
  }

  // the caller's own domain outcomes
  sealed trait Outcome
  case object Written                                           extends Outcome
  final case class Rejected(current: Option[Item])              extends Outcome
  final case class Conflict(codes: List[String])                extends Outcome
  final case class Replaced(previous: Score)                    extends Outcome
  final case class Corrupt(error: DecodingError)                extends Outcome
  final case class DomainError(outcome: Outcome)                extends Exception(outcome.toString)

  private val envLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWithZIO[DynamoDbAsyncClient](c => ZIO.succeed(DynamoDBEnv(c, ZioInterpreter.fromAsyncClient(c)))))

  private def show(label: String)(program: Task[Any]): Task[TestResult] =
    program.exit.flatMap(exit => Console.printLine(s"\n[$label]\n  -> $exit")).as(assertCompletes)

  private def seed(interp: Interpreter[Task], table: Table[Score]) = interp.run(put(table, Score("alice", 42)))

  def spec =
    suite("where / transaction outcomes, handled (examples)")(
      test("1. put.where: fold the Either to a domain outcome") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("1. put.where - fold") {
            seed(interp, table) *>
              interp
                .run(put(table, Score("alice", 99)).where(Score.score === 0))  // Task[Either[ConditionFailed, Unit]]
                .map(_.fold(cf => Rejected(cf.item), _ => Written))
          }
        }
      },
      test("2. put.where + AllOld: use the existing item from the Left") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("2. put.where + AllOld") {
            seed(interp, table) *>
              interp
                .run(
                  put(table, Score("alice", 99))
                    .where(Score.score === 0)
                    .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.AllOld)
                )
                .map {
                  case Right(())                          => Written
                  case Left(ConditionFailed(_, existing)) => Rejected(existing) // raw Item: decode yourself if needed
                }
          }
        }
      },
      test("3. transactWriteItems: inspect the cancellation codes") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("3. transaction - cancellation codes") {
            seed(interp, table) *>
              interp
                .run(transactWriteItems(put(table, Score("alice", 99)).where(Score.score === 0), put(table, Score("bob", 1))))
                .map {
                  case Right(())                                                                    => Written
                  case Left(tc) if tc.reasons.exists(_.code == CancellationCode.ConditionalCheckFailed) =>
                    Rejected(None)
                  case Left(tc)                                                                     => Conflict(tc.reasons.map(_.code.value).toList)
                }
          }
        }
      },
      test("4. where + returning: the nested shape, condition holds") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("4. where + returning (holds)") {
            seed(interp, table) *>
              interp
                .run(put(table, Score("alice", 99)).where(Score.score === 42).returning(ReturnValues.AllOld))
                .map {                                            // Either[ConditionFailed, Option[Either[DecodingError, Score]]]
                  case Left(cf)                => Rejected(cf.item)
                  case Right(None)             => Written           // nothing was there before
                  case Right(Some(Right(prev))) => Replaced(prev)
                  case Right(Some(Left(err)))  => Corrupt(err)
                }
          }
        }
      },
      test("5. sequencing two conditional writes: stop at the first Left") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("5. sequence, short-circuit") {
            seed(interp, table) *>
              interp.run(put(table, Score("bob", 1)).where(Score.id.attributeNotExists)).flatMap {
                case Left(cf) => ZIO.succeed(Rejected(cf.item): Outcome)
                case Right(()) =>
                  interp.run(put(table, Score("alice", 99)).where(Score.score === 0)).map {
                    case Left(cf)  => Rejected(cf.item)
                    case Right(()) => Written
                  }
              }
          }
        }
      },
      test("6. lift the Left into the error channel as the caller's own error") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("6. lift into channel") {
            for {
              _ <- seed(interp, table)
              _ <- interp
                     .run(put(table, Score("alice", 99)).where(Score.score === 0))
                     .flatMap(r => ZIO.fromEither(r.left.map(cf => DomainError(Rejected(cf.item)))))
              _ <- interp.run(put(table, Score("carol", 1)))      // not reached
            } yield Written
          }
        }
      },
      test("7. infrastructure failures alongside: the Either never carries them") {
        withSingleIdKeyTable { (name, interp) =>
          val missing = DdbExprApi.Table[Score](name + "-missing")
          show("7. expected in the value, infrastructure in the channel") {
            interp
              .run(put(missing, Score("alice", 99)).where(Score.score === 0))
              .map(_.fold(cf => Rejected(cf.item), _ => Written))
              .catchSome {
                case e: AwsServiceException if e.isThrottlingException || e.statusCode >= 500 => ZIO.succeed("retry later")
                case _: SdkClientException                                                    => ZIO.succeed("retry later")
                case e: AwsServiceException                                                   => ZIO.succeed(s"not retryable: ${e.awsErrorDetails.errorCode}")
              }
          }
        }
      },
      test("8. Cats Effect: same handling, two writes composed with EitherT") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val ce: CEInterpreter[CEIO] = CEInterpreter.fromAsyncClient[CEIO](env.asyncClient)
          withSingleIdKeyTable { (name, interp) =>
            val table                                       = DdbExprApi.Table[Score](name)
            val program: CEIO[Either[ConditionFailed, Unit]] =
              (for {
                _ <- EitherT(put(table, Score("bob", 1)).where(Score.id.attributeNotExists).execute)
                _ <- EitherT(put(table, Score("alice", 99)).where(Score.score === 0).execute)
              } yield ()).value
            show("8. Cats Effect + EitherT") {
              seed(interp, table) *> ZIO.fromFuture(_ => program.unsafeToFuture()).map(_.fold(cf => Rejected(cf.item), _ => Written))
            }
          }
        }
      }
    ).provideSomeLayer[DynamoDbAsyncClient](envLayer) @@ TestAspect.sequential
}
