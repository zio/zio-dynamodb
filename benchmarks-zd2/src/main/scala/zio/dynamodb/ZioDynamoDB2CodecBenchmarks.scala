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

import org.openjdk.jmh.annotations.{ Scope => JScope, _ }
import zio.schema.{ DeriveSchema, Schema }

import java.util.concurrent.TimeUnit

/**
 * The zio-dynamodb 2.x (zio-schema) side of `DynamoDBCodecBenchmarks`: the same `Person` data,
 * `size`s and JMH settings. It lives in its own module because 2.x and 3.x both define
 * `zio.dynamodb.AttributeValue`, so they must not share a classpath.
 *
 * {{{
 * sbt "benchmarksZd2/Jmh/run -prof gc ZioDynamoDB2CodecBenchmarks"
 * }}}
 */
@State(JScope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
class ZioDynamoDB2CodecBenchmarks {
  import ZioDynamoDB2CodecBenchmarks._

  @Param(Array("1", "10", "100", "1000", "10000", "100000"))
  var size: Int                                  = 1
  var listOfRecords: List[Person]                = uninitialized
  var encodedListOfRecords: List[AttributeValue] = uninitialized

  @Setup
  def setup(): Unit = {
    listOfRecords = (1 to size).map(_ => Person(12345678901L, "John", 30, Some("123 Main St"))).toList
    encodedListOfRecords = listOfRecords.map(encoder(_))
  }

  @Benchmark
  def readingZioSchema: List[Person] =
    encodedListOfRecords.map(av =>
      decoder(av) match {
        case Right(value) => value
        case Left(error)  => sys.error(error.getMessage)
      }
    )

  @Benchmark
  def writingZioSchema: Seq[AttributeValue] = listOfRecords.map(encoder(_))
}

object ZioDynamoDB2CodecBenchmarks {
  final case class Person(id: Long, name: String, age: Int, address: Option[String])

  val schema: Schema[Person]   = DeriveSchema.gen[Person]
  val encoder: Encoder[Person] = Codec.encoder[Person](schema)
  val decoder: Decoder[Person] = Codec.decoder[Person](schema)
}
