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

import software.amazon.awssdk.services.dynamodb.model.{
  ScanRequest,
  ScanResponse,
  TransactGetItemsRequest,
  TransactGetItemsResponse,
  TransactWriteItemsRequest,
  TransactWriteItemsResponse
}
import zio.dynamodb.DynamoDBError.ScanError
import zio.test._
import zio.test.Assertion.{ anything, equalTo, hasField, isSubtype }

import scala.util.Try

object ScanSegmentSpec extends ZIOSpecDefault {

  private val table = "t"

  private def stub(onScan: ScanRequest => ScanResponse = _ => ScanResponse.builder().build()): AwsDynamoDB[DummyIO] =
    new AwsDynamoDB[DummyIO] {
      def getItem(req: software.amazon.awssdk.services.dynamodb.model.GetItemRequest)               = DummyIO.succeed(???)
      def putItem(req: software.amazon.awssdk.services.dynamodb.model.PutItemRequest)               = DummyIO.succeed(???)
      def updateItem(req: software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest)         = DummyIO.succeed(???)
      def deleteItem(req: software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest)         = DummyIO.succeed(???)
      def batchGetItem(req: software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest)     = DummyIO.succeed(???)
      def batchWriteItem(req: software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest) =
        DummyIO.succeed(???)
      def query(req: software.amazon.awssdk.services.dynamodb.model.QueryRequest)                   = DummyIO.succeed(???)
      def scan(req: ScanRequest): DummyIO[ScanResponse]                                             = DummyIO.succeed(onScan(req))
      def createTable(req: software.amazon.awssdk.services.dynamodb.model.CreateTableRequest)       = DummyIO.succeed(???)
      def deleteTable(req: software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest)       = DummyIO.succeed(???)
      def describeTable(req: software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest)   = DummyIO.succeed(???)
      def transactGetItems(req: TransactGetItemsRequest): DummyIO[TransactGetItemsResponse]         = DummyIO.succeed(???)
      def transactWriteItems(req: TransactWriteItemsRequest): DummyIO[TransactWriteItemsResponse]   = DummyIO.succeed(???)
    }

  private def run[A](
    q: DynamoDBQuery[_, A],
    onScan: ScanRequest => ScanResponse = _ => ScanResponse.builder().build()
  ): Try[A] =
    Try(new DummyIOInterpreter(stub(onScan)).run(q).unsafeRun())

  // ---------------------------------------------------------------------------
  // Builder: .segment(index, total)
  // ---------------------------------------------------------------------------

  private val builderSuite = suite(".segment() builder")(
    test("sets segment and totalSegments on Scan") {
      val q: DynamoDBQuery[Any, Page[Item]] = DynamoDBQuery.scan(table, limit = 100).segment(2, 8)
      val seg: Int                          = 2
      val tot: Int                          = 8
      assert(q)(isSubtype[DynamoDBQuery.Scan](hasField("segment", _.segment, equalTo(seg)))) &&
      assert(q)(isSubtype[DynamoDBQuery.Scan](hasField("totalSegments", _.totalSegments, equalTo(tot))))
    },
    test("default Scan has segment=0, totalSegments=1") {
      val q: DynamoDBQuery[Any, Page[Item]] = DynamoDBQuery.scan(table, limit = 100)
      val seg: Int                          = 0
      val tot: Int                          = 1
      assert(q)(isSubtype[DynamoDBQuery.Scan](hasField("segment", _.segment, equalTo(seg)))) &&
      assert(q)(isSubtype[DynamoDBQuery.Scan](hasField("totalSegments", _.totalSegments, equalTo(tot))))
    },
    test("no-op on non-Scan queries") {
      val base = DynamoDBQuery.getItem(table, PrimaryKey("id" -> "x"))
      assertTrue(base.segment(1, 4).eq(base))
    },
    test("propagates through Map") {
      val q        = DynamoDBQuery.scan(table, limit = 100).map(_.items).segment(1, 4)
      val seg: Int = 1
      val tot: Int = 4
      val inner    = q.asInstanceOf[DynamoDBQuery.Map[_, _]].query
      assert(inner)(isSubtype[DynamoDBQuery.Scan](hasField("segment", _.segment, equalTo(seg)))) &&
      assert(inner)(isSubtype[DynamoDBQuery.Scan](hasField("totalSegments", _.totalSegments, equalTo(tot))))
    }
  )

  // ---------------------------------------------------------------------------
  // Codec: segment fields appear in the AWS ScanRequest
  // ---------------------------------------------------------------------------

  private val codecSuite = suite("toScanRequest codec")(
    test("segment(1, 4) sets segment=1 and totalSegments=4 on the SDK request") {
      var captured: ScanRequest = null
      run(
        DynamoDBQuery.scan(table, limit = 50).segment(1, 4),
        req => { captured = req; ScanResponse.builder().build() }
      )
      assertTrue(
        captured.segment() == 1 &&
          captured.totalSegments() == 4
      )
    },
    test("segment(0, 6) sets segment=0 and totalSegments=6") {
      var captured: ScanRequest = null
      run(
        DynamoDBQuery.scan(table, limit = 50).segment(0, 6),
        req => { captured = req; ScanResponse.builder().build() }
      )
      assertTrue(
        captured.segment() == 0 &&
          captured.totalSegments() == 6
      )
    },
    test("default (totalSegments=1) does not set segment or totalSegments on the SDK request") {
      var captured: ScanRequest = null
      run(
        DynamoDBQuery.scan(table, limit = 50),
        req => { captured = req; ScanResponse.builder().build() }
      )
      assertTrue(
        captured.segment() == null &&
          captured.totalSegments() == null
      )
    }
  )

  // ---------------------------------------------------------------------------
  // Validation: invalid segment parameters are rejected by the interpreter
  // ---------------------------------------------------------------------------

  private val validationSuite = suite("interpreter validates segment parameters")(
    test("totalSegments=0 fails with ScanValidationError") {
      val result = run(DynamoDBQuery.scan(table, limit = 50).segment(0, 0))
      assert(result.failed.get)(isSubtype[ScanError.ScanValidationError](anything)) &&
      assertTrue(result.failed.get.getMessage.contains("totalSegments"))
    },
    test("segment >= totalSegments fails with ScanValidationError") {
      val result = run(DynamoDBQuery.scan(table, limit = 50).segment(4, 4))
      assert(result.failed.get)(isSubtype[ScanError.ScanValidationError](anything)) &&
      assertTrue(result.failed.get.getMessage.contains("segment=4"))
    },
    test("negative segment fails with ScanValidationError") {
      val result = run(DynamoDBQuery.scan(table, limit = 50).segment(-1, 4))
      assert(result.failed.get)(isSubtype[ScanError.ScanValidationError](anything))
    },
    test("segment=0, totalSegments=1 (default) succeeds") {
      val result = run(DynamoDBQuery.scan(table, limit = 50))
      assertTrue(result.isSuccess)
    },
    test("segment=0, totalSegments=2 (minimum parallel) succeeds") {
      val result = run(DynamoDBQuery.scan(table, limit = 50).segment(0, 2))
      assertTrue(result.isSuccess)
    },
    test("last valid segment index succeeds") {
      val result = run(DynamoDBQuery.scan(table, limit = 50).segment(3, 4))
      assertTrue(result.isSuccess)
    }
  )

  def spec = suite("ScanSegmentSpec")(
    builderSuite,
    codecSuite,
    validationSuite
  )
}
