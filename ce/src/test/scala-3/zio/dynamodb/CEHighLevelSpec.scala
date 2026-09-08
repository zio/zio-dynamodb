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

import cats.effect.{ IO, Resource }
import munit.CatsEffectSuite
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import scala.concurrent.duration.*
import software.amazon.awssdk.auth.credentials.{ AwsBasicCredentials, StaticCredentialsProvider }
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import java.net.URI
import java.util.UUID
import zio.blocks.chunk.Chunk
import zio.blocks.schema.{ CompanionOptics, Modifier, Schema }
import zio.dynamodb.blocks.ddbexpr.{ DdbExpr, DdbExprApi, DdbKeyExpr }
import zio.dynamodb.blocks.ddbexpr.DdbExprApi._
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._
import zio.dynamodb.blocks.ddbexpr.DdbExpr.{
  DdbExprBoolSyntax,
  OpticDdbExprOps,
  OpticStringDdbExprOps,
  OpticUpdateOps,
  SchemaExprBoolBridge
}
import zio.dynamodb.blocks.schema.DynamoDBCodecDeriver
import zio.dynamodb.ExecuteSyntax._

/**
 * Showcases the HL (DdbExprApi) in Cats Effect / Scala 3 style.
 *
 * Features demonstrated:
 *   - `derives Schema` (Scala 3 syntax)
 *   - Scala 3 `enum` as a field type
 *   - Opaque types encoding as their underlying primitive
 *   - `@Modifier.fieldNaming` per-type snake_case mapping
 *   - deriver config attached to a `Table` via `.deriving` (per-field rename)
 *   - put / get / update / deleteFrom / scan / query (imported from DdbExprApi._)
 *   - Condition expressions: attributeNotExists, between, in, &&, ! (Not)
 *   - Update expressions: .set, .add, combined (+)
 *   - ExecuteSyntax: `.execute` dispatches via the ambient implicit Interpreter[IO]
 */
class CEHighLevelSpec extends CatsEffectSuite:

  override val munitIOTimeout: Duration = 3.minutes

  // ---- infrastructure -------------------------------------------------------

  private val containerResource: Resource[IO, DynamoDbAsyncClient] =
    Resource
      .make(IO {
        val c = new GenericContainer(DockerImageName.parse("amazon/dynamodb-local:latest")) {}
        c.addExposedPort(8000)
        c.setCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
        c.waitingFor(Wait.forListeningPort())
        c.start()
        c
      })(c => IO(c.stop()))
      .map { c =>
        val endpoint = URI.create(s"http://${c.getHost}:${c.getMappedPort(8000)}")
        val creds    = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
        DynamoDbAsyncClient
          .builder()
          .endpointOverride(endpoint)
          .credentialsProvider(creds)
          .region(Region.US_EAST_1)
          .build()
      }

  private val clientFixture = ResourceSuiteLocalFixture("dynamodb-client-hl", containerResource)

  override def munitFixtures = List(clientFixture)

  private def tableResource(keySchema: KeySchema, attrDefs: NonEmptySet[AttributeDefinition])(using
    Interpreter[IO]
  ): Resource[IO, String] =
    val tableName = s"test-${UUID.randomUUID()}"
    Resource.make(
      DynamoDBQuery.createTable(tableName, keySchema, attrDefs, BillingMode.PayPerRequest).execute.as(tableName)
    )(name => DynamoDBQuery.deleteTable(name).execute.handleError(_ => ()))

  private def singleKeyTable[A](f: Interpreter[IO] ?=> String => IO[A]): IO[A] =
    given Interpreter[IO] = CEInterpreter.fromAsyncClient(clientFixture())
    tableResource(KeySchema("id"), NonEmptySet(AttributeDefinition.attrDefnString("id"))).use(f)

  private def compoundKeyTable[A](f: Interpreter[IO] ?=> String => IO[A]): IO[A] =
    given Interpreter[IO] = CEInterpreter.fromAsyncClient(clientFixture())
    tableResource(
      KeySchema("id", "ts"),
      NonEmptySet(AttributeDefinition.attrDefnString("id"), AttributeDefinition.attrDefnString("ts"))
    ).use(f)

  // ---- models ---------------------------------------------------------------

  // Scala 3 derives Schema — no boilerplate implicit val needed
  case class Article(id: String, title: String, views: Int) derives Schema

  object Article extends CompanionOptics[Article]:
    val id    = $(_.id)
    val title = $(_.title)
    val views = $(_.views)

  // Scala 3 enum as a field type
  enum Priority derives Schema:
    case Low, Medium, High

  case class Task(id: String, name: String, priority: Priority, tags: List[String]) derives Schema

  object Task extends CompanionOptics[Task]:
    val id       = $(_.id)
    val name     = $(_.name)
    val priority = $(_.priority)
    val tags     = $(_.tags)

  // Opaque types: each encodes as its underlying primitive, not a Map wrapper
  opaque type Handle = String
  object Handle:
    given Schema[Handle]         = Schema.string
    def apply(s: String): Handle = s

  opaque type Points = Int
  object Points:
    given Schema[Points]      = Schema.int
    def apply(n: Int): Points = n

  case class Player(id: String, handle: Handle, score: Points) derives Schema

  object Player extends CompanionOptics[Player]:
    val id     = $(_.id)
    val handle = $(_.handle)
    val score  = $(_.score)

  // Per-type field-name strategy via @Modifier.fieldNaming — no deriver config needed
  @Modifier.fieldNaming("snake_case")
  case class UserProfile(id: String, firstName: String, lastName: String) derives Schema

  object UserProfile extends CompanionOptics[UserProfile]:
    val id = $(_.id)

  // Compound partition+sort key model
  case class LogEntry(id: String, ts: String, message: String) derives Schema

  object LogEntry extends CompanionOptics[LogEntry]:
    val id      = $(_.id)
    val ts      = $(_.ts)
    val message = $(_.message)

  // Per-field rename applied through `.deriving` at the call site below — the config is not
  // attached to the model or its companion.
  case class Member(id: String, name: String, age: Int) derives Schema

  object Member extends CompanionOptics[Member]:
    val id = $(_.id)

  // ---- CRUD tests -----------------------------------------------------------

  test("DdbExprApi.put and get round-trip a `derives Schema` record") {
    singleKeyTable { tableName =>
      val table   = Table[Article](tableName)
      val article = Article("a1", "Hello world", 0)
      for
        _      <- put(table, article).execute
        result <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield assertEquals(result, Right(article))
    }
  }

  test("DdbExprApi.get returns Left(ValueNotFound) when item is absent") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for result <- get[Article](table)(Article.id.partitionKey === "missing").execute
      yield assert(result.isLeft)
    }
  }

  test("DdbExprApi.deleteFrom removes the item") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _      <- put(table, Article("a1", "To delete", 0)).execute
        _      <- deleteFrom[Article](table)(Article.id.partitionKey === "a1").execute
        result <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield assert(result.isLeft)
    }
  }

  // ---- condition expression tests -------------------------------------------

  test("put condition: attributeNotExists prevents overwrite on second write") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _       <- put(table, Article("a1", "First", 0)).where(Article.id.attributeNotExists).execute
        blocked <- put(table, Article("a1", "Second", 0)).where(Article.id.attributeNotExists).execute.attempt
        result  <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield {
        assert(blocked.isLeft)
        assertEquals(result, Right(Article("a1", "First", 0)))
      }
    }
  }

  test("put condition: between passes when existing value is in range") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _      <- put(table, Article("a1", "Original", 50)).execute
        _      <- put(table, Article("a1", "Updated", 0)).where(Article.views.between(0, 100)).execute
        result <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield assertEquals(result, Right(Article("a1", "Updated", 0)))
    }
  }

  test("put condition: in(..) passes when value is one of the allowed values") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _      <- put(table, Article("a1", "Draft", 0)).execute
        _      <- put(table, Article("a1", "Published", 0)).where(Article.title.in("Draft", "Review")).execute
        result <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield assertEquals(result.map(_.title), Right("Published"))
    }
  }

  test("put condition: && (DdbExpr And) — both predicates must hold") {
    singleKeyTable { tableName =>
      val table                               = Table[Article](tableName)
      val bothHold: DdbExpr[Article, Boolean] = Article.views > 10 && Article.title === "Hello"
      for
        _       <- put(table, Article("a1", "Hello", 42)).execute
        // Both hold → write succeeds
        _       <- put(table, Article("a1", "Changed", 0)).where(bothHold).execute
        // First holds but second does not → write fails
        blocked <- put(table, Article("a1", "Blocked", 0)).where(bothHold).execute.attempt
        result  <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield {
        assert(blocked.isLeft)
        assertEquals(result.map(_.title), Right("Changed"))
      }
    }
  }

  test("put condition: ! (Not) — negated predicate") {
    singleKeyTable { tableName =>
      val table                                        = Table[Article](tableName)
      val notNineNinetyNine: DdbExpr[Article, Boolean] = !(Article.views === 999)
      val notZero: DdbExpr[Article, Boolean]           = !(Article.views === 0)
      for
        _       <- put(table, Article("a1", "Existing", 0)).execute
        // !(views === 999) holds when views == 0, so write succeeds
        _       <- put(table, Article("a1", "Replaced", 0)).where(notNineNinetyNine).execute
        // !(views === 0) does NOT hold when views == 0, so write is rejected
        blocked <- put(table, Article("a1", "Blocked", 0)).where(notZero).execute.attempt
        result  <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield {
        assert(blocked.isLeft)
        assertEquals(result.map(_.title), Right("Replaced"))
      }
    }
  }

  // ---- update expression tests ----------------------------------------------

  test("update: .set changes a field value") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _      <- put(table, Article("a1", "Original title", 0)).execute
        _      <- update(table)(Article.id.partitionKey === "a1")(Article.title.set("New title")).execute
        result <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield assertEquals(result.map(_.title), Right("New title"))
    }
  }

  test("update: .add increments a numeric field") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _      <- put(table, Article("a1", "Counting", 10)).execute
        _      <- update(table)(Article.id.partitionKey === "a1")(Article.views.add(5)).execute
        result <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield assertEquals(result.map(_.views), Right(15))
    }
  }

  test("update: combined action via + (set title and add views in one call)") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _      <- put(table, Article("a1", "Original", 0)).execute
        _      <- update(table)(Article.id.partitionKey === "a1")(
                    Article.title.set("Updated") + Article.views.add(1)
                  ).execute
        result <- get[Article](table)(Article.id.partitionKey === "a1").execute
      yield assertEquals(result, Right(Article("a1", "Updated", 1)))
    }
  }

  // ---- Scala 3 enum tests ---------------------------------------------------

  test("Scala 3 enum field round-trips through DdbExprApi") {
    singleKeyTable { tableName =>
      val table = Table[Task](tableName)
      val task  = Task("t1", "Write Scala 3 specs", Priority.High, List("ci", "scala3"))
      for
        _      <- put(table, task).execute
        result <- get[Task](table)(Task.id.partitionKey === "t1").execute
      yield assertEquals(result, Right(task))
    }
  }

  test("Scala 3 enum field: updating a non-enum field preserves the enum value in the stored record") {
    singleKeyTable { tableName =>
      val table = Table[Task](tableName)
      for
        _      <- put(table, Task("t1", "Original", Priority.High, List("a"))).execute
        _      <- update(table)(Task.id.partitionKey === "t1")(Task.name.set("Renamed")).execute
        result <- get[Task](table)(Task.id.partitionKey === "t1").execute
      yield assertEquals(result, Right(Task("t1", "Renamed", Priority.High, List("a"))))
    }
  }

  // ---- opaque type tests ----------------------------------------------------

  test("opaque type fields encode as their underlying primitives (not Map wrappers)") {
    singleKeyTable { tableName =>
      val table  = Table[Player](tableName)
      val player = Player("p1", Handle("alice99"), Points(999))
      for
        _      <- put(table, player).execute
        result <- get[Player](table)(Player.id.partitionKey === "p1").execute
        raw    <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "p1")).execute
      yield {
        assertEquals(result, Right(player))
        // Handle encodes as AttributeValue.String, not {"value": "alice99"}
        assertEquals(raw.flatMap(_.get[String]("handle").toOption), Some("alice99"))
        // Points encodes as AttributeValue.Number, not {"value": 999}
        assertEquals(raw.flatMap(_.get[Int]("score").toOption), Some(999))
      }
    }
  }

  // ---- modifier tests -------------------------------------------------------

  test("@Modifier.fieldNaming snake_case: DynamoDB stores snake_case attribute keys") {
    singleKeyTable { tableName =>
      val table   = Table[UserProfile](tableName)
      val profile = UserProfile("u1", "John", "Doe")
      for
        _      <- put(table, profile).execute
        raw    <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "u1")).execute
        result <- get[UserProfile](table)(UserProfile.id.partitionKey === "u1").execute
      yield {
        assertEquals(result, Right(profile))
        assert(raw.exists(_.map.contains("first_name")))
        assert(raw.exists(_.map.contains("last_name")))
        assert(raw.exists(!_.map.contains("firstName")))
      }
    }
  }

  test("Table.deriving per-field rename reflected in DynamoDB item and round-trips") {
    singleKeyTable { tableName =>
      val memberType = summon[Schema[Member]].reflect.typeId
      val table      = Table[Member](tableName).deriving(
        _.withModifier(memberType, "name", Modifier.rename("fullName"))
          .withModifier(memberType, "age", Modifier.rename("yearsOld"))
      )
      val member     = Member("m1", "Alice", 30)
      for
        _      <- put(table, member).execute
        raw    <- DynamoDBQuery.getItem(tableName, PrimaryKey("id" -> "m1")).execute
        result <- get[Member](table)(Member.id.partitionKey === "m1").execute
      yield {
        assertEquals(result, Right(member))
        assert(raw.exists(_.map.contains("fullName")))
        assert(raw.exists(_.map.contains("yearsOld")))
        assert(raw.exists(!_.map.contains("name")))
        assert(raw.exists(!_.map.contains("age")))
      }
    }
  }

  // ---- scan tests -----------------------------------------------------------

  test("DdbExprApi.scan returns all items in the table") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _    <- put(table, Article("a1", "First", 10)).execute
        _    <- put(table, Article("a2", "Second", 20)).execute
        _    <- put(table, Article("a3", "Third", 30)).execute
        page <- scan[Article](table, limit = 10).execute
        decoded = page.items.collect { case Right(a) => a }
      yield assertEquals(decoded.size, 3)
    }
  }

  test("DdbExprApi.scan with between filter returns only matching items") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _    <- put(table, Article("a1", "Low", 5)).execute
        _    <- put(table, Article("a2", "Mid", 15)).execute
        _    <- put(table, Article("a3", "High", 100)).execute
        page <- scan[Article](table, limit = 10).filter(Article.views.between(10, 50)).execute
        decoded = page.items.collect { case Right(a) => a }
      yield assertEquals(decoded.map(_.id).toSet, Set("a2"))
    }
  }

  test("DdbExprApi.scan pagination: limit caps results and startKey enables next page") {
    singleKeyTable { tableName =>
      val table = Table[Article](tableName)
      for
        _     <- put(table, Article("a1", "A", 1)).execute
        _     <- put(table, Article("a2", "B", 2)).execute
        _     <- put(table, Article("a3", "C", 3)).execute
        page1 <- scan[Article](table, limit = 1).execute
        page2 <- scan[Article](table, limit = 10).startKey(page1.lastEvaluatedKey).execute
      yield {
        assertEquals(page1.items.size, 1)
        assert(page1.lastEvaluatedKey.isDefined)
        assertEquals(page1.items.size + page2.items.size, 3)
      }
    }
  }

  // ---- query tests ----------------------------------------------------------

  test("DdbExprApi.query with partitionKey === returns only items for that partition key") {
    compoundKeyTable { tableName =>
      val table = Table[LogEntry](tableName)
      for
        _    <- put(table, LogEntry("user1", "2024-01-01", "login")).execute
        _    <- put(table, LogEntry("user1", "2024-01-02", "update")).execute
        _    <- put(table, LogEntry("user2", "2024-01-01", "login")).execute
        page <- query[LogEntry](table, limit = 10).whereKey(LogEntry.id.partitionKey === "user1").execute
        decoded = page.items.collect { case Right(e) => e }
      yield assertEquals(decoded.size, 2)
    }
  }

  test("DdbExprApi.query with partitionKey === and sortKey > filters to sort-key range") {
    compoundKeyTable { tableName =>
      val table = Table[LogEntry](tableName)
      for
        _    <- put(table, LogEntry("user1", "2024-01-01", "login")).execute
        _    <- put(table, LogEntry("user1", "2024-01-02", "update")).execute
        _    <- put(table, LogEntry("user1", "2024-01-03", "logout")).execute
        page <- query[LogEntry](table, limit = 10)
                  .whereKey(LogEntry.id.partitionKey === "user1" && LogEntry.ts.sortKey > "2024-01-01")
                  .execute
        decoded = page.items.collect { case Right(e) => e }
      yield {
        assertEquals(decoded.size, 2)
        assert(decoded.forall(_.ts > "2024-01-01"))
      }
    }
  }

  test("DdbExprApi.query with sortOrder(ascending = false) returns items in descending order") {
    compoundKeyTable { tableName =>
      val table = Table[LogEntry](tableName)
      for
        _    <- put(table, LogEntry("user1", "2024-01-01", "login")).execute
        _    <- put(table, LogEntry("user1", "2024-01-02", "update")).execute
        _    <- put(table, LogEntry("user1", "2024-01-03", "logout")).execute
        page <- query[LogEntry](table, limit = 10)
                  .whereKey(LogEntry.id.partitionKey === "user1")
                  .sortOrder(ascending = false)
                  .execute
        decoded = page.items.collect { case Right(e) => e }
      yield assertEquals(decoded.map(_.ts), Chunk("2024-01-03", "2024-01-02", "2024-01-01"))
    }
  }
