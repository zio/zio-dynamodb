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

import org.openjdk.jmh.annotations._
import org.scanamo._
import org.scanamo.DynamoReadError.describe
import org.scanamo.generic.semiauto._
import org.scanamo.{ DynamoValue => ScanamoValue }
import zio.blocks.schema.Schema
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

/**
 * Compares Scanamo and ZioBlocks DynamoDBCodec for encoding and decoding a record with a large
 * binary payload field. Models the event-sourcing pattern where an aggregate state is serialised
 * to protobuf bytes and stored as a DynamoDB Binary attribute.
 *
 * `payloadBytes` controls the size of the payload array; the benchmark encodes/decodes a single
 * `EventStoreRecord` per call to isolate codec overhead from iteration overhead.
 *
 * Both codecs use the DynamoDB native Binary attribute type (B) for the payload field.
 *
 * ==Running the benchmark==
 *
 * All payload sizes, throughput mode:
 * {{{
 * sbt "benchmarks/jmh:run DynamoDBBinaryCodecBenchmarks"
 * }}}
 *
 * Single payload size (e.g. 10 000 bytes):
 * {{{
 * sbt "benchmarks/jmh:run -p payloadBytes=10000 DynamoDBBinaryCodecBenchmarks"
 * }}}
 *
 * With GC allocation profiling (bytes/op):
 * {{{
 * sbt "benchmarks/jmh:run -prof gc DynamoDBBinaryCodecBenchmarks"
 * }}}
 *
 * Single benchmark method:
 * {{{
 * sbt "benchmarks/jmh:run DynamoDBBinaryCodecBenchmarks.writingZioBlocks"
 * }}}
 */
class DynamoDBBinaryCodecBenchmarks extends BaseBenchmark {
  import BinaryBenchmarkDomain._

  @Param(Array("100", "1000", "10000", "100000"))
  var payloadBytes: Int = 100

  var record: EventStoreRecord         = _
  var encodedForBlocks: AttributeValue = _
  var encodedForScanamo: ScanamoValue  = _

  @Setup
  def setup(): Unit = {
    val rng     = new java.util.Random(42L)
    val payload = new Array[Byte](payloadBytes)
    rng.nextBytes(payload)
    record = EventStoreRecord("aggregate-001", "event#0000000001", payload)
    encodedForBlocks = blocksCodec.encoder(record)
    encodedForScanamo = scanamoFormat.write(record)
  }

  @Benchmark
  def writingZioBlocks: AttributeValue = blocksCodec.encoder(record)

  @Benchmark
  def writingScanamo: ScanamoValue = scanamoFormat.write(record)

  @Benchmark
  def readingZioBlocks: EventStoreRecord =
    blocksCodec.decoder(encodedForBlocks) match {
      case Right(v) => v
      case Left(e)  => sys.error(e.message)
    }

  @Benchmark
  def readingScanamo: EventStoreRecord =
    scanamoFormat.read(encodedForScanamo) match {
      case Right(v) => v
      case Left(e)  => sys.error(describe(e))
    }
}

final case class EventStoreRecord(id: String, sk: String, payload: Array[Byte])
object EventStoreRecord {
  implicit val schema: Schema[EventStoreRecord] = Schema.derived
}

object BinaryBenchmarkDomain {
  implicit val scanamoFormat: DynamoFormat[EventStoreRecord] = deriveDynamoFormat

  val blocksCodec: DynamoDBCodec[EventStoreRecord] =
    EventStoreRecord.schema.deriving(DynamoDBCodecDeriver).derive
}
