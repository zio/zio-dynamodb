package zio.dynamodb.benchmarks

import zio.Runtime
import zio.dynamodb.DynamoDBExecutor
import zio.dynamodb.TestDynamoDBExecutor
import zio.Unsafe
import zio.ZIO
import zio.dynamodb.DynamoDBError

private[benchmarks] final class ZIODynamoDbBenchmark private (runtime: Runtime.Scoped[TestDynamoDBExecutor with DynamoDBExecutor]) {
  def run(program: ZIO[TestDynamoDBExecutor with DynamoDBExecutor, DynamoDBError, Unit]): Unit =
    Unsafe.unsafe(implicit unsafe => runtime.unsafe.run(program).getOrThrow())

  def tearDown(): Unit = Unsafe.unsafe(implicit u => runtime.unsafe.shutdown())
}
object ZIODynamoDbBenchmark                                                                              {
  def unsafeMake(): ZIODynamoDbBenchmark = {
    val runtime = Unsafe.unsafe(implicit unsafe => Runtime.unsafe.fromLayer(DynamoDBExecutor.test("Person" -> "id")))
    new ZIODynamoDbBenchmark(runtime)
  }
}
