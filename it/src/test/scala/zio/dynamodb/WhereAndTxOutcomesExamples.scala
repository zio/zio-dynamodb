/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import cats.effect.{ IO => CEIO }
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
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

// Examples, not tests: each handles a conditional write / transaction outcome and prints it.
// Failures fail the effect (ConditionFailed / TransactionCancelled / raw SDK exceptions), so the
// handling is a narrow handler: ZIO `catchSome`, Cats Effect `recoverWith`.
object WhereAndTxOutcomesExamples extends DynamoDBLocalSpec {

  final case class Score(id: String, score: Int)
  object Score extends CompanionOptics[Score] {
    implicit val schema: Schema[Score] = Schema.derived
    val id: Lens[Score, String]        = $(_.id)
    val score: Lens[Score, Int]        = $(_.score)
  }

  // the caller's own domain outcomes
  sealed trait Outcome
  case object Written                              extends Outcome
  final case class Rejected(current: Option[Item]) extends Outcome
  final case class Conflict(codes: List[String])   extends Outcome
  final case class Replaced(previous: Score)       extends Outcome
  final case class Corrupt(error: DecodingError)   extends Outcome

  private val envLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWithZIO[DynamoDbAsyncClient](c => ZIO.succeed(DynamoDBEnv(c, ZioInterpreter.fromAsyncClient(c)))))

  private def show(label: String)(program: Task[Any]): Task[TestResult] =
    program.exit.flatMap(exit => Console.printLine(s"\n[$label]\n  -> $exit")).as(assertCompletes)

  private def seed(interp: Interpreter[Task], table: Table[Score]) = interp.run(put(table, Score("alice", 42)))

  def spec =
    suite("where / transaction outcomes, handled (examples)")(
      test("1. put.where: map success, catch only ConditionFailed") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("1. put.where - narrow handler") {
            seed(interp, table) *>
              interp
                .run(put(table, Score("alice", 99)).where(Score.score === 0)) // Task[Unit]
                .as(Written: Outcome)
                .catchSome { case cf: ConditionFailed => ZIO.succeed(Rejected(cf.item)) }
          }
        }
      },
      test("2. put.where: use the existing item from ConditionFailed (returned by default)") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("2. put.where - existing item") {
            seed(interp, table) *>
              interp
                .run(
                  put(table, Score("alice", 99))
                    .where(Score.score === 0)
                )
                .as(Written: Outcome)
                .catchSome { case ConditionFailed(_, existing) => ZIO.succeed(Rejected(existing)) } // raw Item
          }
        }
      },
      test("3. transactWriteItems: inspect the cancellation codes") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("3. transaction - cancellation codes") {
            seed(interp, table) *>
              interp
                .run(
                  DynamoDBQuery.transactWriteItems(
                    put(table, Score("alice", 99)).where(Score.score === 0),
                    put(table, Score("bob", 1))
                  )
                )
                .as(Written: Outcome)
                .catchSome {
                  case tc: TransactionCancelled
                      if tc.reasons.exists(_.code == CancellationCode.ConditionalCheckFailed) =>
                    ZIO.succeed(Rejected(None))
                  case tc: TransactionCancelled =>
                    ZIO.succeed(Conflict(tc.reasons.map(_.code.value).toList))
                }
          }
        }
      },
      test("4. where + returning: the returned item, condition holds") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("4. where + returning (holds)") {
            seed(interp, table) *>
              interp
                .run(put(table, Score("alice", 99)).where(Score.score === 42).returning(ReturnValues.AllOld))
                .map[Outcome] { // Option[Either[DecodingError, Score]]
                  case None              => Written // nothing was there before
                  case Some(Right(prev)) => Replaced(prev)
                  case Some(Left(err))   => Corrupt(err)
                }
                .catchSome { case cf: ConditionFailed => ZIO.succeed(Rejected(cf.item)) }
          }
        }
      },
      test("5. sequencing two conditional writes: the first failure stops the rest") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("5. sequence, short-circuit") {
            (for {
              _ <- seed(interp, table)
              _ <- interp.run(put(table, Score("bob", 1)).where(Score.id.attributeNotExists))
              _ <- interp.run(put(table, Score("alice", 99)).where(Score.score === 0))
              _ <- interp.run(put(table, Score("carol", 1))) // not reached
            } yield Written: Outcome).catchSome { case cf: ConditionFailed => ZIO.succeed(Rejected(cf.item)) }
          }
        }
      },
      test("6. result discarded: the failure still stops the program") {
        withSingleIdKeyTable { (name, interp) =>
          val table = DdbExprApi.Table[Score](name)
          show("6. discarded, still loud") {
            for {
              _ <- seed(interp, table)
              _ <- interp.run(put(table, Score("alice", 99)).where(Score.score === 0)) // nothing to discard
              _ <- interp.run(put(table, Score("carol", 1)))                           // not reached
            } yield Written
          }
        }
      },
      test("7. infrastructure failures alongside, in the same handler") {
        withSingleIdKeyTable { (name, interp) =>
          val missing = DdbExprApi.Table[Score](name + "-missing")
          show("7. expected + infrastructure in one handler") {
            interp
              .run(put(missing, Score("alice", 99)).where(Score.score === 0))
              .as("written")
              .catchSome {
                case cf: ConditionFailed                                                      => ZIO.succeed(s"rejected: ${cf.item}")
                case e: AwsServiceException if e.isThrottlingException || e.statusCode >= 500 =>
                  ZIO.succeed("retry later")
                case _: SdkClientException                                                    => ZIO.succeed("retry later")
                case e: AwsServiceException                                                   =>
                  ZIO.succeed(s"not retryable: ${e.awsErrorDetails.errorCode}")
              }
          }
        }
      },
      test("8. Cats Effect: same handling with recoverWith, two writes in sequence") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          implicit val ce: CEInterpreter[CEIO] = CEInterpreter.fromAsyncClient[CEIO](env.asyncClient)
          withSingleIdKeyTable { (name, interp) =>
            val table                  = DdbExprApi.Table[Score](name)
            val program: CEIO[Outcome] =
              (for {
                _ <- put(table, Score("bob", 1)).where(Score.id.attributeNotExists).execute
                _ <- put(table, Score("alice", 99)).where(Score.score === 0).execute
              } yield Written: Outcome).recoverWith { case cf: ConditionFailed =>
                (Rejected(cf.item): Outcome).pure[CEIO]
              }
            show("8. Cats Effect + recoverWith") {
              seed(interp, table) *> ZIO.fromFuture(_ => program.unsafeToFuture())
            }
          }
        }
      }
    ).provideSomeLayer[DynamoDbAsyncClient](envLayer) @@ TestAspect.sequential
}
