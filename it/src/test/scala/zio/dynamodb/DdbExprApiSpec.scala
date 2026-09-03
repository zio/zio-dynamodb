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
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import zio._
import zio.blocks.chunk.Chunk
import zio.blocks.schema.{ CompanionOptics, Lens, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExprApi, DdbKeyExpr }
import zio.dynamodb.blocks.ddbexpr.DdbExprApi._
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._
import zio.dynamodb.blocks.ddbexpr.DdbExpr.{ DdbExprBoolSyntax, OpticDdbExprOps, OpticUpdateOps }
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect

/**
 * Integration tests for [[DdbExprApi]].
 *
 *  Focuses on the correctness guarantee that sealed-trait literal values in
 *  filter/condition expressions are encoded via [[zio.dynamodb.blocks.schema.DynamoDBCodec]]
 *  directly, not through a [[zio.blocks.schema.DynamicValue]] round-trip that would lose
 *  the `enumValuesAsStrings` encoding rule.
 */
object DdbExprApiSpec extends DynamoDBLocalSpec {

  // ── Models ───────────────────────────────────────────────────────────────────

  // All-no-field sealed trait: DynamoDBCodecDeriver encodes each case as
  // AttributeValue.String — e.g. Priority.High → "High".
  private sealed trait Priority
  private object Priority {
    case object Low  extends Priority
    case object High extends Priority
    implicit val schema: Schema[Priority] = Schema.derived
  }

  private case class Task(id: String, priority: Priority)
  private object Task extends CompanionOptics[Task] {
    implicit val schema: Schema[Task]  = Schema.derived
    val id: Lens[Task, String]         = $(_.id)
    val priority: Lens[Task, Priority] = $(_.priority)
  }

  // ── Env layer ─────────────────────────────────────────────────────────────────

  private val envLayer: URLayer[DynamoDbAsyncClient, DynamoDBEnv] =
    ZLayer(ZIO.serviceWith[DynamoDbAsyncClient](client => DynamoDBEnv(client, ZioInterpreter.fromAsyncClient(client))))

  // ── Tests ─────────────────────────────────────────────────────────────────────

  // ── Test suites ───────────────────────────────────────────────────────────────

  private val putGetTests: Spec[DynamoDBEnv, Throwable] =
    suite("put + get round-trip with all-no-field sealed trait")(
      test("DdbExprApi.get returns Right(Task) with Priority.High decoded correctly") {
        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Task](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Task("t1", Priority.High)))
            result <- interpreter.run(DdbExprApi.get[Task](table)(Task.id.partitionKey === "t1"))
          } yield assertTrue(result == Right(Task("t1", Priority.High)))
        }
      },
      test("DdbExprApi.get returns Left(ValueNotFound) for absent item") {
        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Task](tableName)
          for {
            result <- interpreter.run(DdbExprApi.get[Task](table)(Task.id.partitionKey === "ghost"))
          } yield assert(result)(isLeft(isSubtype[DynamoDBError.ItemError.ValueNotFound](anything)))
        }
      }
    )

  private val compoundKeyTests: Spec[DynamoDBEnv, Throwable] =
    suite("compound partition+sort key")(
      test("get returns Right(A) with partitionKey + sortKey equality") {
        final case class Event(id: String, year: String, title: String)
        object Event extends CompanionOptics[Event] {
          implicit val schema: Schema[Event] = Schema.derived
          val id: Lens[Event, String]        = $(_.id)
          val year: Lens[Event, String]      = $(_.year)
        }

        withIdAndYearKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Event](tableName)
          val event = Event("alice", "2024", "birthday")
          for {
            _      <- interpreter.run(DdbExprApi.put(table, event))
            result <- interpreter.run(
                        DdbExprApi.get[Event](table)(Event.id.partitionKey === "alice" && Event.year.sortKey === "2024")
                      )
          } yield assertTrue(result == Right(event))
        }
      },
      test("get returns Left(ValueNotFound) for missing compound key item") {
        final case class Event(id: String, year: String, title: String)
        object Event extends CompanionOptics[Event] {
          implicit val schema: Schema[Event] = Schema.derived
          val id: Lens[Event, String]        = $(_.id)
          val year: Lens[Event, String]      = $(_.year)
        }

        withIdAndYearKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Event](tableName)
          for {
            result <- interpreter.run(
                        DdbExprApi.get[Event](table)(Event.id.partitionKey === "alice" && Event.year.sortKey === "2024")
                      )
          } yield assert(result)(isLeft(isSubtype[DynamoDBError.ItemError.ValueNotFound](anything)))
        }
      }
    )

  private val conditionExprTests: Spec[DynamoDBEnv, Throwable] =
    suite("condition expressions on write operations")(
      test("put with attributeNotExists succeeds when item is absent") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)).where(Score.id.attributeNotExists))
            result <- interpreter.run(DdbExprApi.get[Score](table)(Score.id.partitionKey === "alice"))
          } yield assertTrue(result == Right(Score("alice", 42)))
        }
      },
      test("put with attributeNotExists fails when item already exists") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            result <-
              interpreter.run(DdbExprApi.put(table, Score("alice", 99)).where(Score.id.attributeNotExists)).either
          } yield assert(result)(isLeft(anything))
        }
      },
      test("put with between condition succeeds when existing value is in range") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
          val score: Lens[Score, Int]        = $(_.score)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 99)).where(Score.score.between(0, 50)))
            result <- interpreter.run(DdbExprApi.get[Score](table)(Score.id.partitionKey === "alice"))
          } yield assertTrue(result == Right(Score("alice", 99)))
        }
      },
      test("put with between condition fails when existing value is out of range") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
          val score: Lens[Score, Int]        = $(_.score)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            result <-
              interpreter.run(DdbExprApi.put(table, Score("alice", 99)).where(Score.score.between(50, 100))).either
          } yield assert(result)(isLeft(anything))
        }
      },
      test("put with in condition succeeds when existing value matches") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
          val score: Lens[Score, Int]        = $(_.score)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 99)).where(Score.score.in(10, 42, 77)))
            result <- interpreter.run(DdbExprApi.get[Score](table)(Score.id.partitionKey === "alice"))
          } yield assertTrue(result == Right(Score("alice", 99)))
        }
      }
    )

  private val enumFilterTests: Spec[DynamoDBEnv, Throwable] =
    suite("DdbExpr filter — enum literal encoding correctness")(
      test("scan.filter finds item where priority === Priority.High") {
        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Task](tableName)
          for {
            _    <- interpreter.run(DdbExprApi.put(table, Task("t1", Priority.High)))
            _    <- interpreter.run(DdbExprApi.put(table, Task("t2", Priority.Low)))
            page <- interpreter.run(
                      DdbExprApi
                        .scan[Task](table, 10)
                        .filter(Task.priority === Priority.High)
                    )
          } yield assertTrue(page.items.collect { case Right(t) => t } == Chunk(Task("t1", Priority.High)))
        }
      }
    )

  private val queryTests: Spec[DynamoDBEnv, Throwable] =
    suite("query with DdbKeyExpr key condition")(
      test("query.whereKey(partitionKey) returns items for that partition key") {
        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Task](tableName)
          for {
            _    <- interpreter.run(DdbExprApi.put(table, Task("t1", Priority.High)))
            _    <- interpreter.run(DdbExprApi.put(table, Task("t2", Priority.Low)))
            page <- interpreter.run(
                      DdbExprApi
                        .query[Task](table, 10)
                        .whereKey(Task.id.partitionKey === "t1")
                    )
          } yield assertTrue(page.items.collect { case Right(t) => t } == Chunk(Task("t1", Priority.High)))
        }
      },
      test("query with partitionKey + sortKey > filters to range") {
        final case class Event(id: String, year: String, title: String)
        object Event extends CompanionOptics[Event] {
          implicit val schema: Schema[Event] = Schema.derived
          val id: Lens[Event, String]        = $(_.id)
          val year: Lens[Event, String]      = $(_.year)
        }

        withIdAndYearKeyTable { (tableName, interpreter) =>
          val table  = DdbExprApi.Table[Event](tableName)
          val events = List(
            Event("alice", "2021", "a"),
            Event("alice", "2022", "b"),
            Event("alice", "2023", "c")
          )
          for {
            _    <- ZIO.foreach(events)(e => interpreter.run(DdbExprApi.put(table, e)))
            page <- interpreter.run(
                      DdbExprApi
                        .query[Event](table, 10)
                        .whereKey(Event.id.partitionKey === "alice" && Event.year.sortKey > "2021")
                    )
            decoded = page.items.collect { case Right(a) => a }
          } yield assertTrue(decoded.size == 2 && decoded.forall(_.year > "2021"))
        }
      },
      test("query limit caps the page size and returns a non-empty lastEvaluatedKey") {
        final case class Event(id: String, year: String, title: String)
        object Event extends CompanionOptics[Event] {
          implicit val schema: Schema[Event] = Schema.derived
          val id: Lens[Event, String]        = $(_.id)
          val year: Lens[Event, String]      = $(_.year)
        }

        withIdAndYearKeyTable { (tableName, interpreter) =>
          val table  = DdbExprApi.Table[Event](tableName)
          val events = List(Event("alice", "2021", "a"), Event("alice", "2022", "b"), Event("alice", "2023", "c"))
          for {
            _    <- ZIO.foreach(events)(e => interpreter.run(DdbExprApi.put(table, e)))
            page <- interpreter.run(
                      DdbExprApi.query[Event](table, 2).whereKey(Event.id.partitionKey === "alice")
                    )
          } yield assertTrue(page.items.size == 2 && page.lastEvaluatedKey.isDefined)
        }
      }
    )

  private val scanTests: Spec[DynamoDBEnv, Throwable] =
    suite("scan")(
      test("scan returns all items in the table") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          val items = List(Score("a", 10), Score("b", 20), Score("c", 30))
          for {
            _    <- ZIO.foreach(items)(s => interpreter.run(DdbExprApi.put(table, s)))
            page <- interpreter.run(DdbExprApi.scan[Score](table, 100))
            decoded = page.items.collect { case Right(a) => a }
          } yield assertTrue(decoded.toSet == items.toSet && page.lastEvaluatedKey.isEmpty)
        }
      },
      test("scan with filter returns only matching items") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
          val score: Lens[Score, Int]        = $(_.score)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          val items = List(Score("a", 5), Score("b", 15), Score("c", 25))
          for {
            _    <- ZIO.foreach(items)(s => interpreter.run(DdbExprApi.put(table, s)))
            page <- interpreter.run(DdbExprApi.scan[Score](table, 100).filter(Score.score.between(10, 20)))
            decoded = page.items.collect { case Right(a) => a }
          } yield assertTrue(decoded.toSet == Set(Score("b", 15)))
        }
      }
    )

  private val updateTests: Spec[DynamoDBEnv, Throwable] =
    suite("update")(
      test("update on single partition key table sets field") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
          val score: Lens[Score, Int]        = $(_.score)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            _      <-
              interpreter.run(DdbExprApi.update[Score](table)(Score.id.partitionKey === "alice")(Score.score.set(100)))
            result <- interpreter.run(DdbExprApi.get[Score](table)(Score.id.partitionKey === "alice"))
          } yield assertTrue(result == Right(Score("alice", 100)))
        }
      },
      test("update on compound key table sets field") {
        final case class Event(id: String, year: String, title: String)
        object Event extends CompanionOptics[Event] {
          implicit val schema: Schema[Event] = Schema.derived
          val id: Lens[Event, String]        = $(_.id)
          val year: Lens[Event, String]      = $(_.year)
          val title: Lens[Event, String]     = $(_.title)
        }

        withIdAndYearKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Event](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Event("alice", "2024", "birthday")))
            _      <- interpreter.run(
                        DdbExprApi.update[Event](table)(Event.id.partitionKey === "alice" && Event.year.sortKey === "2024")(
                          Event.title.set("anniversary")
                        )
                      )
            result <- interpreter.run(
                        DdbExprApi.get[Event](table)(Event.id.partitionKey === "alice" && Event.year.sortKey === "2024")
                      )
          } yield assertTrue(result == Right(Event("alice", "2024", "anniversary")))
        }
      },
      test("update with condition that holds succeeds") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
          val score: Lens[Score, Int]        = $(_.score)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            _      <- interpreter.run(
                        DdbExprApi
                          .update[Score](table)(Score.id.partitionKey === "alice")(Score.score.set(99))
                          .where(Score.score.between(0, 50) && Score.id.attributeExists)
                      )
            result <- interpreter.run(DdbExprApi.get[Score](table)(Score.id.partitionKey === "alice"))
          } yield assertTrue(result == Right(Score("alice", 99)))
        }
      },
      test("update with condition that fails raises ConditionalCheckFailedException") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
          val score: Lens[Score, Int]        = $(_.score)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            result <- interpreter
                        .run(
                          DdbExprApi
                            .update[Score](table)(Score.id.partitionKey === "alice")(Score.score.set(99))
                            .where(Score.score.between(50, 100))
                        )
                        .either
          } yield assert(result)(isLeft(isSubtype[ConditionalCheckFailedException](anything)))
        }
      }
    )

  private val deleteTests: Spec[DynamoDBEnv, Throwable] =
    suite("deleteFrom")(
      test("deleteFrom on single partition key table removes the item") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            _      <- interpreter.run(DdbExprApi.deleteFrom[Score](table)(Score.id.partitionKey === "alice"))
            result <- interpreter.run(DdbExprApi.get[Score](table)(Score.id.partitionKey === "alice"))
          } yield assert(result)(isLeft(isSubtype[DynamoDBError.ItemError.ValueNotFound](anything)))
        }
      },
      test("deleteFrom on compound key table removes the item") {
        final case class Event(id: String, year: String, title: String)
        object Event extends CompanionOptics[Event] {
          implicit val schema: Schema[Event] = Schema.derived
          val id: Lens[Event, String]        = $(_.id)
          val year: Lens[Event, String]      = $(_.year)
        }

        withIdAndYearKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Event](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Event("alice", "2024", "birthday")))
            _      <-
              interpreter.run(
                DdbExprApi.deleteFrom[Event](table)(Event.id.partitionKey === "alice" && Event.year.sortKey === "2024")
              )
            result <- interpreter.run(
                        DdbExprApi.get[Event](table)(Event.id.partitionKey === "alice" && Event.year.sortKey === "2024")
                      )
          } yield assert(result)(isLeft(isSubtype[DynamoDBError.ItemError.ValueNotFound](anything)))
        }
      },
      test("deleteFrom with condition that holds removes the item") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
          val score: Lens[Score, Int]        = $(_.score)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            _      <- interpreter.run(
                        DdbExprApi
                          .deleteFrom[Score](table)(Score.id.partitionKey === "alice")
                          .where(Score.score.between(0, 50) && Score.id.attributeExists)
                      )
            result <- interpreter.run(DdbExprApi.get[Score](table)(Score.id.partitionKey === "alice"))
          } yield assert(result)(isLeft(isSubtype[DynamoDBError.ItemError.ValueNotFound](anything)))
        }
      },
      test("deleteFrom with condition that fails raises ConditionalCheckFailedException") {
        final case class Score(id: String, score: Int)
        object Score extends CompanionOptics[Score] {
          implicit val schema: Schema[Score] = Schema.derived
          val id: Lens[Score, String]        = $(_.id)
          val score: Lens[Score, Int]        = $(_.score)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Score](tableName)
          for {
            _      <- interpreter.run(DdbExprApi.put(table, Score("alice", 42)))
            result <- interpreter
                        .run(
                          DdbExprApi
                            .deleteFrom[Score](table)(Score.id.partitionKey === "alice")
                            .where(Score.score.between(50, 100))
                        )
                        .either
          } yield assert(result)(isLeft(isSubtype[ConditionalCheckFailedException](anything)))
        }
      }
    )

  private val nativeSetTests: Spec[DynamoDBEnv, Throwable] =
    suite("native Set types")(
      test("Set[String] field round-trips through DynamoDB as StringSet") {
        final case class Tagged(id: String, tags: Set[String])
        object Tagged extends CompanionOptics[Tagged] {
          implicit val schema: Schema[Tagged] = Schema.derived
          val id: Lens[Tagged, String]        = $(_.id)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Tagged](tableName)
          val item  = Tagged("t1", Set("alpha", "beta", "gamma"))
          for {
            _      <- interpreter.run(DdbExprApi.put(table, item))
            result <- interpreter.run(DdbExprApi.get[Tagged](table)(Tagged.id.partitionKey === "t1"))
          } yield assertTrue(result == Right(item))
        }
      },
      test("Set[Int] field round-trips through DynamoDB as NumberSet") {
        final case class Scores(id: String, values: Set[Int])
        object Scores extends CompanionOptics[Scores] {
          implicit val schema: Schema[Scores] = Schema.derived
          val id: Lens[Scores, String]        = $(_.id)
        }

        withSingleIdKeyTable { (tableName, interpreter) =>
          val table = DdbExprApi.Table[Scores](tableName)
          val item  = Scores("s1", Set(10, 20, 30))
          for {
            _      <- interpreter.run(DdbExprApi.put(table, item))
            result <- interpreter.run(DdbExprApi.get[Scores](table)(Scores.id.partitionKey === "s1"))
          } yield assertTrue(result == Right(item))
        }
      }
    )

  def spec =
    suite("DdbExprApi IT")(
      putGetTests,
      compoundKeyTests,
      conditionExprTests,
      enumFilterTests,
      queryTests,
      scanTests,
      updateTests,
      deleteTests,
      nativeSetTests
    )
      .provideSome[DynamoDbAsyncClient](envLayer) @@
      TestAspect.sequential
}
