package zio.dynamodb.benchmarks

import org.openjdk.jmh.annotations._
import zio.dynamodb._
//import zio.{ Scope => _, _ }

import java.util.concurrent.TimeUnit
import zio.schema.DeriveSchema
import zio.schema.Schema

@State(Scope.Thread)
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 15)
@Warmup(iterations = 15)
@Fork(2)
class RoundTripBenchmark {
  import RoundTripBenchmark._

  @Param(Array("500"))
  var repetitions: Int = _

  @Setup
  def setup(): Unit =
    zioDynamoDB = ZIODynamoDbBenchmark.unsafeMake()

  @TearDown
  def tearDown(): Unit =
    zioDynamoDB.tearDown()

  @Benchmark
  def zioDDB(): Unit =
    zioDynamoDB.run {
      for {
        _ <- DynamoDBQuery.put("Person", Person("1", "John")).execute
        _ <- DynamoDBQuery.get("Person")(Person.id.partitionKey === "1").execute
      } yield ()
    }

  private var zioDynamoDB: ZIODynamoDbBenchmark = _
}

object RoundTripBenchmark {
  final case class Person(id: String, name: String)
  object Person {
    implicit val schema: Schema.CaseClass2[String, String, Person] = DeriveSchema.gen[Person]
    val (id, name)                                                 = ProjectionExpression.accessors[Person]
  }
}
