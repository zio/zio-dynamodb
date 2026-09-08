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
import org.openjdk.jmh.annotations._
import org.scanamo._
import org.scanamo.syntax._
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model._
import zio.blocks.schema.CompanionOptics
import zio.dynamodb.BinaryBenchmarkDomain._
import zio.dynamodb.DynamoDBError.ItemError
import zio.dynamodb.blocks.ddbexpr.{ DdbExprApi, DdbKeyExpr }
import zio.dynamodb.blocks.ddbexpr.DdbExprApi._
import zio.dynamodb.blocks.ddbexpr.DdbKeyExpr._

import java.lang.reflect.{ InvocationHandler, Method, Proxy }
import scala.collection.JavaConverters._

/**
 * Full effect-stack benchmark for the event-store access pattern.
 *
 * Domain: EventStoreRecord(id: String, sk: String, payload: Array[Byte])
 *   - PK = id (aggregateId)
 *   - SK = sk (sequence number / event type)
 *   - payload = protobuf-serialised aggregate state as Array[Byte]
 *
 * The payload field exercises the Array[Byte] → AttributeValue.Binary zero-copy
 * codec path on both encode (put) and decode (get).
 *
 * Compares two approaches:
 *   - Scanamo   — sync client wrapped in IO.delay
 *   - DdbExprApi — HL API with codec caching, same Optic call ergonomics
 *
 * payloadBytes is parameterized so the benchmark covers a representative small event
 * (1 KB) and a larger one (64 KB) typical of complex aggregates.
 *
 * Run:
 * {{{
 * sbt "benchmarks/jmh:run EventStoreBench"
 * sbt "benchmarks/jmh:run EventStoreBench -prof gc"
 * sbt "benchmarks/jmh:run EventStoreBench -p payloadBytes=1024"
 * }}}
 */
@Warmup(iterations = 10, time = 1, timeUnit = java.util.concurrent.TimeUnit.SECONDS)
class EventStoreBench extends BaseBenchmark {

  private val TABLE    = "event-store"
  private val ddbTable = DdbExprApi.Table[EventStoreRecord](TABLE)

  @Param(Array("1024", "65536"))
  var payloadBytes: Int = _

  private var record: EventStoreRecord = _

  // ── Scanamo ─────────────────────────────────────────────────────────────

  private var scanamo: Scanamo                                  = _
  private var scanamoTable: org.scanamo.Table[EventStoreRecord] = _

  // ── blocks-dynamodb ──────────────────────────────────────────────────────

  private var interpreter: CEInterpreter = _

  private object EventRecordOps extends CompanionOptics[EventStoreRecord] {
    val id = $(_.id)
    val sk = $(_.sk)
  }

  @Setup def setup(): Unit = {
    val rng     = new java.util.Random(42L)
    val payload = new Array[Byte](payloadBytes)
    rng.nextBytes(payload)
    record = EventStoreRecord("aggregate-001", "event#000000001", payload)

    // ── Canned AWS responses ──────────────────────────────────────────────

    val awsItem: java.util.Map[String, AttributeValue] = Map(
      "id"      -> AttributeValue.builder().s(record.id).build(),
      "sk"      -> AttributeValue.builder().s(record.sk).build(),
      "payload" -> AttributeValue.builder().b(SdkBytes.fromByteArray(record.payload)).build()
    ).asJava

    val cannedGet = GetItemResponse.builder().item(awsItem).build()
    val cannedPut = PutItemResponse.builder().build()

    // ── blocks stub ───────────────────────────────────────────────────────

    val stubDynamo: AwsDynamoDB[IO] = new AwsDynamoDB[IO] {
      private def unsupported                                                                = IO.raiseError[Nothing](new UnsupportedOperationException("stub"))
      def getItem(req: GetItemRequest): IO[GetItemResponse]                                  = IO.pure(cannedGet)
      def putItem(req: PutItemRequest): IO[PutItemResponse]                                  = IO.pure(cannedPut)
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
    interpreter = new CEInterpreter(stubDynamo)

    // ── Scanamo stub ──────────────────────────────────────────────────────

    val stubHandler: InvocationHandler = new InvocationHandler {
      def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
        method.getName match {
          case "getItem"     => cannedGet
          case "putItem"     => cannedPut
          case "serviceName" => "dynamodb"
          case "close"       => null
          case name          => throw new UnsupportedOperationException(s"stub: $name")
        }
    }
    val stubClient                     = Proxy
      .newProxyInstance(
        classOf[DynamoDbClient].getClassLoader,
        Array(classOf[DynamoDbClient]),
        stubHandler
      )
      .asInstanceOf[DynamoDbClient]
    scanamo = Scanamo(stubClient)
    scanamoTable = org.scanamo.Table[EventStoreRecord](TABLE)(scanamoFormat)
  }

  // ── Benchmarks ──────────────────────────────────────────────────────────

  /** Scanamo: compound-key get wrapped in IO.delay. */
  @Benchmark def scanamoGet: Option[Either[DynamoReadError, EventStoreRecord]] =
    IO.delay(scanamo.exec(scanamoTable.get("id" === record.id and "sk" === record.sk))).unsafeRunSync()

  /** Scanamo: put wrapped in IO.delay. */
  @Benchmark def scanamoPut: Unit =
    IO.delay(scanamo.exec(scanamoTable.put(record))).unsafeRunSync()

  /** DdbExprApi: cached HL get — same Optic ergonomics; codec derived once per type. */
  @Benchmark def blocksGet: Either[ItemError, EventStoreRecord] =
    interpreter
      .run(
        DdbExprApi.get(ddbTable)(
          EventRecordOps.id.partitionKey === record.id && EventRecordOps.sk.sortKey === record.sk
        )
      )
      .unsafeRunSync()

  /** DdbExprApi: cached HL put — codec derived once per type. */
  @Benchmark def blocksPut: Option[EventStoreRecord] =
    interpreter
      .run(DdbExprApi.put(ddbTable, record))
      .unsafeRunSync()
}
