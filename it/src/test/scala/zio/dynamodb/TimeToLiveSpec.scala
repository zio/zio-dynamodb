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

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.{ TimeToLiveSpecification, UpdateTimeToLiveRequest }
import zio._
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.blocks.schema.ddbexpr.{ DdbExprApi, DdbUpdateExpr }
import zio.dynamodb.blocks.schema.ddbexpr.dsl._
import zio.test.Assertion._
import zio.test._

import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * A time to live attribute kept in the model with the High-Level API. DynamoDB only expires an
 * item whose attribute is a Number of epoch seconds, and ignores any other format, whereas an
 * `Instant` field is written as a string - hence the `EpochSeconds` wrapper. Whether items are
 * actually deleted is DynamoDB's own behaviour, so it is not tested here.
 */
object TimeToLiveSpec extends DynamoDBLocalSpec {

  final case class EpochSeconds(value: Instant)
  object EpochSeconds {
    private def read(seconds: Long): EpochSeconds = EpochSeconds(Instant.ofEpochSecond(seconds))
    private def write(ttl: EpochSeconds): Long    = ttl.value.getEpochSecond

    implicit val schema: Schema[EpochSeconds] = Schema[Long].transform[EpochSeconds](read, write)
  }

  final case class Session(id: String, userId: String, updatedAt: Instant, ttl: EpochSeconds)
  object Session extends CompanionOptics[Session] {
    implicit val schema: Schema[Session]  = Schema.derived
    val id: Lens[Session, String]         = $(_.id)
    val userId: Lens[Session, String]     = $(_.userId)
    val updatedAt: Lens[Session, Instant] = $(_.updatedAt)
    val ttl: Lens[Session, EpochSeconds]  = $(_.ttl)

    def expiry(at: Instant): EpochSeconds = EpochSeconds(at.plus(90, ChronoUnit.DAYS))

    def create(id: String, userId: String, now: Instant): Session = Session(id, userId, now, expiry(now))

    // An update only writes the attributes it names, so the expiry has to be refreshed explicitly.
    def touch(now: Instant): DdbUpdateExpr[Session] = updatedAt.set(now) + ttl.set(expiry(now))
  }

  private val created = Instant.parse("2026-01-01T00:00:00Z")
  private val later   = created.plus(1, ChronoUnit.DAYS)

  private val envLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWith[DynamoDbAsyncClient](client => DynamoDBEnv(client, ZioInterpreter.fromAsyncClient(client))))

  private def key = Session.id.partitionKey === "s1"

  private def number(ttl: EpochSeconds): AttributeValue =
    AttributeValue.Number(BigDecimal(ttl.value.getEpochSecond))

  private def storedTtl(tableName: String, interpreter: Interpreter[Task]): Task[Option[AttributeValue]] =
    interpreter.run(DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "s1"))).map(_.flatMap(_.map.get("ttl")))

  def spec =
    suite("time to live attribute with the High-Level API")(
      test("the table accepts the reserved word ttl as its time to live attribute") {
        ZIO.serviceWithZIO[DynamoDBEnv] { env =>
          withSingleIdKeyTable { (tableName, _) =>
            val specification = TimeToLiveSpecification.builder().attributeName("ttl").enabled(true).build()
            val request       =
              UpdateTimeToLiveRequest.builder().tableName(tableName).timeToLiveSpecification(specification).build()
            ZIO
              .fromCompletableFuture(env.asyncClient.updateTimeToLive(request))
              .map(response => assertTrue(response.timeToLiveSpecification().attributeName() == "ttl"))
          }
        }
      },
      test("put stores the calculated expiry as a Number of epoch seconds") {
        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Session](tableName)
          for {
            _      <- interpreter.run(put(table, Session.create("s1", "u1", created)))
            stored <- storedTtl(tableName, interpreter)
          } yield assert(stored)(isSome(equalTo(number(Session.expiry(created)))))
        }
      },
      test("get decodes the expiry back into the wrapper") {
        withSingleIdKeyTable { (tableName, interpreter) =>
          val table   = DdbExprApi.Table[Session](tableName)
          val session = Session.create("s1", "u1", created)
          for {
            _      <- interpreter.run(put(table, session))
            result <- interpreter.run(get[Session](table)(key))
          } yield assertTrue(result.contains(Right(session)))
        }
      },
      test("the update helper refreshes both the date and the expiry") {
        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Session](tableName)
          for {
            _      <- interpreter.run(put(table, Session.create("s1", "u1", created)))
            _      <- interpreter.run(update(table)(key)(Session.touch(later)))
            result <- interpreter.run(get[Session](table)(key))
            stored <- storedTtl(tableName, interpreter)
          } yield assertTrue(result.contains(Right(Session.create("s1", "u1", later)))) &&
            assert(stored)(isSome(equalTo(number(Session.expiry(later)))))
        }
      },
      test("an update that only names the date leaves the expiry unchanged") {
        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Session](tableName)
          for {
            _      <- interpreter.run(put(table, Session.create("s1", "u1", created)))
            _      <- interpreter.run(update(table)(key)(Session.updatedAt.set(later)))
            stored <- storedTtl(tableName, interpreter)
          } yield assert(stored)(isSome(equalTo(number(Session.expiry(created)))))
        }
      },
      test("a condition on the expiry encodes the wrapper and aliases the reserved word") {
        withSingleIdKeyTable { (tableName, interpreter) =>
          val table       = DdbExprApi.Table[Session](tableName)
          val conditional = update(table)(key)(Session.touch(later)).where(Session.ttl === Session.expiry(created))
          for {
            _     <- interpreter.run(put(table, Session.create("s1", "u1", created)))
            _     <- interpreter.run(conditional)
            fails <- interpreter.run(conditional).either
          } yield assert(fails)(isLeft(isSubtype[DynamoDBError.ItemError.ConditionFailed](anything)))
        }
      }
    ).provideSome[DynamoDbAsyncClient](envLayer) @@ TestAspect.sequential
}
