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

import scala.compiletime.uninitialized

import dynosaur.Schema.WriteError
import dynosaur.{ DynamoValue => DynosaurValue }
import org.openjdk.jmh.annotations._
import org.scanamo.DynamoReadError.describe
import org.scanamo.{ DynamoValue => ScanamoValue }
import zio.dynamodb.blocks.schema.{ DynamoDBCodec, DynamoDBCodecDeriver }

/**
 * `DynamoDBCodecBenchmarks` for `BigRecord` (26 fields, nested records and sum types) instead of the
 * flat `Person`, for zio-blocks, Scanamo and Dynosaur.
 *
 * {{{
 * sbt "benchmarks/Jmh/run -prof gc BigRecordCodecBenchmarks"
 * }}}
 */
class BigRecordCodecBenchmarks extends BaseBenchmark {
  import BenchmarkDomain._
  import BigRecordCodecBenchmarks._

  @Param(Array("1", "1000"))
  var size: Int                                 = 1
  var records: List[BigRecord]                  = uninitialized
  var encodedForZioBlocks: List[AttributeValue] = uninitialized
  var encodedForScanamo: List[ScanamoValue]     = uninitialized
  var encodedForDynosaur: List[DynosaurValue]   = uninitialized

  @Setup
  def setup(): Unit = {
    records = List.fill(size)(BigRecord.sample)
    encodedForZioBlocks = records.map(bigRecordCodec.encoder(_))
    encodedForScanamo = records.map(ScanamoCodec.bigRecord.write)
    encodedForDynosaur =
      records.map(r => DynosaurSchema.bigRecordSchema.write(r).fold(e => sys.error(e.getMessage), identity))

    // A codec that loses data would make the comparison meaningless, so check each one round-trips.
    val sample = BigRecord.sample
    require(bigRecordCodec.decoder(encodedForZioBlocks.head) == Right(sample), "zio-blocks round trip")
    require(ScanamoCodec.bigRecord.read(encodedForScanamo.head) == Right(sample), "Scanamo round trip")
    require(DynosaurSchema.bigRecordSchema.read(encodedForDynosaur.head) == Right(sample), "Dynosaur round trip")
  }

  @Benchmark
  def readingScanamo: List[BigRecord] =
    encodedForScanamo.map(av =>
      ScanamoCodec.bigRecord.read(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(describe(error))
      }
    )

  @Benchmark
  def readingDynosaur: List[BigRecord] =
    encodedForDynosaur.map(av =>
      DynosaurSchema.bigRecordSchema.read(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

  @Benchmark
  def readingZioBlocks: List[BigRecord] =
    encodedForZioBlocks.map(av =>
      bigRecordCodec.decoder(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

  @Benchmark
  def writingScanamo: Seq[ScanamoValue] = records.map(ScanamoCodec.bigRecord.write)

  @Benchmark
  def writingDynosaur: Seq[Either[WriteError, DynosaurValue]] = records.map(DynosaurSchema.bigRecordSchema.write)

  @Benchmark
  def writingZioBlocks: Seq[AttributeValue] = records.map(bigRecordCodec.encoder(_))
}

object BigRecordCodecBenchmarks {
  import BenchmarkDomain.BigRecord

  val bigRecordCodec: DynamoDBCodec[BigRecord] = BigRecord.blocksSchema.deriving(DynamoDBCodecDeriver).derive
}
