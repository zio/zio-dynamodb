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

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scanamo._
import org.scanamo.syntax._
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.blocks.schema.CompanionOptics
import zio.dynamodb.BenchmarkDomain._
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.blocks.ddbexpr.{ DdbExprApi, DdbKeyExpr }
import zio.dynamodb.blocks.ddbexpr.DdbExprApi._
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._

import org.openjdk.jmh.annotations.{ Benchmark, Setup, Warmup }
import java.lang.reflect.{ InvocationHandler, Method, Proxy }
import scala.collection.JavaConverters._

/**
 * Full effect-stack throughput for the High Level API using canned responses.
 *
 * Measures:
 *   - Query / expression construction
 *   - Schema codec encode / decode
 *   - CE IO monad overhead
 *   - Any library-specific wiring
 *
 * Network and DynamoDB are excluded: both backends return a pre-built response
 * immediately without any I/O.
 *
 * Comparison: blocks-dynamodb (CE) vs Scanamo (sync wrapped in IO.delay).
 *
 * Run with:
 * {{{
 *   sbt "benchmarks/jmh:run CeEffectStackBench"
 * }}}
 */
@Warmup(iterations = 10, time = 1, timeUnit = java.util.concurrent.TimeUnit.SECONDS)
class CeEffectStackBench extends BaseBenchmark {

  private val TABLE    = "users"
  private val personId = 12345678901L
  private val person   = Person(personId, "John", 30, Some("123 Main St"))

  // ── Canned AWS responses ────────────────────────────────────────────────

  private val awsItem: java.util.Map[String, AttributeValue] = Map(
    "id"      -> AttributeValue.builder().n(personId.toString).build(),
    "name"    -> AttributeValue.builder().s("John").build(),
    "age"     -> AttributeValue.builder().n("30").build(),
    "address" -> AttributeValue.builder().s("123 Main St").build()
  ).asJava

  private val cannedGetResponse: GetItemResponse =
    GetItemResponse.builder().item(awsItem).build()

  private val cannedPutResponse: PutItemResponse =
    PutItemResponse.builder().build()

  // ── blocks-dynamodb CE interpreter ─────────────────────────────────────

  // Stub AwsDynamoDB[IO]: returns canned responses as already-completed IO.pure
  // values — zero async dispatch, isolates effect-stack overhead.
  private val stubDynamo: AwsDynamoDB[IO] = new AwsDynamoDB[IO] {
    private def unsupported                                                                = IO.raiseError[Nothing](new UnsupportedOperationException("stub"))
    def getItem(req: GetItemRequest): IO[GetItemResponse]                                  = IO.pure(cannedGetResponse)
    def putItem(req: PutItemRequest): IO[PutItemResponse]                                  = IO.pure(cannedPutResponse)
    def updateItem(req: UpdateItemRequest): IO[UpdateItemResponse]                         = unsupported
    def deleteItem(req: DeleteItemRequest): IO[DeleteItemResponse]                         = unsupported
    def batchGetItem(req: BatchGetItemRequest): IO[BatchGetItemResponse]                   = unsupported
    def batchWriteItem(req: BatchWriteItemRequest): IO[BatchWriteItemResponse]             = unsupported
    def query(req: QueryRequest): IO[QueryResponse]                                        = unsupported
    def scan(req: ScanRequest): IO[ScanResponse]                                           = unsupported
    def createTable(req: CreateTableRequest): IO[CreateTableResponse]                      = unsupported
    def deleteTable(req: DeleteTableRequest): IO[DeleteTableResponse]                      = unsupported
    def describeTable(req: DescribeTableRequest): IO[DescribeTableResponse]                = unsupported
    def transactGetItems(req: TransactGetItemsRequest): IO[TransactGetItemsResponse]       = unsupported
    def transactWriteItems(req: TransactWriteItemsRequest): IO[TransactWriteItemsResponse] = unsupported
  }

  private val interpreter = new CEInterpreter(stubDynamo)

  // Optic for DdbExprApi.get — provides Person.id lens via CompanionOptics macro.
  private object PersonOps extends CompanionOptics[Person] {
    val id   = $(_.id)
    val name = $(_.name)
  }

  // ── Scanamo sync wrapped in IO.delay ────────────────────────────────────

  // Stub DynamoDbClient (sync) via Java Proxy — only getItem / putItem need to
  // return real responses; all other operations are unused in these benchmarks.
  private val stubSyncHandler: InvocationHandler = new InvocationHandler {
    def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
      method.getName match {
        case "getItem"     => cannedGetResponse
        case "putItem"     => cannedPutResponse
        case "serviceName" => "dynamodb"
        case "close"       => null
        case name          => throw new UnsupportedOperationException(s"Stub: $name not implemented")
      }
  }

  private val stubSyncClient: DynamoDbClient = Proxy
    .newProxyInstance(
      classOf[DynamoDbClient].getClassLoader,
      Array(classOf[DynamoDbClient]),
      stubSyncHandler
    )
    .asInstanceOf[DynamoDbClient]

  private val scanamo = Scanamo(stubSyncClient)

  private val scanamoTable = Table[Person](TABLE)(ScanamoCodec.person)

  // ── Step 2: pre-built queries (set in @Setup) ───────────────────────────

  private var prebuiltGetQuery: DynamoDBQuery[Person, Either[ItemError, Person]] = _
  private var prebuiltPutQuery: DynamoDBQuery[Person, Option[Person]]            = _

  @Setup def setup(): Unit = {
    prebuiltGetQuery = DdbExprApi.get[Person](TABLE)(PersonOps.id.partitionKey === personId)
    prebuiltPutQuery = DdbExprApi.put(TABLE, person)
  }

  // ── Benchmarks ──────────────────────────────────────────────────────────

  /** blocks-dynamodb: typed HL get via CE interpreter with canned response. */
  @Benchmark def blocksGet: Either[ItemError, Person] =
    interpreter
      .run(DdbExprApi.get[Person](TABLE)(PersonOps.id.partitionKey === personId))
      .unsafeRunSync()

  /** blocks-dynamodb: typed HL put via CE interpreter with canned response. */
  @Benchmark def blocksPut: Option[Person] =
    interpreter
      .run(DdbExprApi.put(TABLE, person))
      .unsafeRunSync()

  /** Scanamo: typed get wrapped in IO.delay. */
  @Benchmark def scanamoGet: Option[Either[DynamoReadError, Person]] =
    IO.delay(scanamo.exec(scanamoTable.get("id" === personId))).unsafeRunSync()

  /** Scanamo: typed put wrapped in IO.delay. */
  @Benchmark def scanamoPut: Unit =
    IO.delay(scanamo.exec(scanamoTable.put(person))).unsafeRunSync()

  // ── Step 2: pre-built ───────────────────────────────────────────────────

  /** Step 2: query pre-built in Setup — only IO + interpreter overhead per call. */
  @Benchmark def blocksGetPrebuilt: Either[ItemError, Person] =
    interpreter.run(prebuiltGetQuery).unsafeRunSync()

  /** Step 2: query pre-built in Setup — only IO + interpreter overhead per call. */
  @Benchmark def blocksPutPrebuilt: Option[Person] =
    interpreter.run(prebuiltPutQuery).unsafeRunSync()

  // ── Construction-only (no run) — isolates DdbExprApi.get/put allocation ────

  /** Construction only: DdbExprApi.get, never interpreted/run. Isolates query-building cost. */
  @Benchmark def blocksGetConstructOnly: DynamoDBQuery[Person, Either[ItemError, Person]] =
    DdbExprApi.get[Person](TABLE)(PersonOps.id.partitionKey === personId)

  /** Construction only: DdbExprApi.put, never interpreted/run. Isolates query-building cost. */
  @Benchmark def blocksPutConstructOnly: DynamoDBQuery[Person, Option[Person]] =
    DdbExprApi.put(TABLE, person)

}
