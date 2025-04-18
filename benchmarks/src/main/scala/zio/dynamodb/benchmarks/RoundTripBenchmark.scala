package zio.dynamodb.benchmarks

import org.openjdk.jmh.annotations._
import zio.dynamodb._

import java.util.concurrent.TimeUnit

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class RoundTripBenchmark {

  @Param(Array("500"))
  var repetitions: Int = _

  @Setup
  def setup(): Unit =
    zioDdbBenchmark = ZIODynamoDbBenchmark.unsafeMake()

  @TearDown
  def tearDown(): Unit =
    zioDdbBenchmark.tearDown()

  @Benchmark
  def zioDdbApiBenchmark(): Unit =
    zioDdbBenchmark.run {
      for { // TODO: use repetitions
        _ <- DynamoDBQuery.put(Person.tableName, Person("1", "John")).execute
        _ <- DynamoDBQuery.get("Person")(Person.id.partitionKey === "1").execute
      } yield ()
    }

  private var zioDdbBenchmark: ZIODynamoDbBenchmark = _
}
