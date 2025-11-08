package zio.dynamodb.benchmarks.blocks

import zio.test.{ assertTrue, ZIOSpecDefault }

object ListOfRecordsBenchmarkSpec extends ZIOSpecDefault {
  def spec =
    suite("ListOfRecordsBenchmarkSpec")(
      test("writing: zio blocks equals zio schema") {
        val benchmark = new ListOfRecordsBenchmark()
        benchmark.setup()
        assertTrue(benchmark.writingZioBlocks == benchmark.writingZioSchema)
      },
      test("reading: zio blocks equals zio schema") {
        val benchmark = new ListOfRecordsBenchmark()
        benchmark.setup()
        assertTrue(benchmark.readingZioBlocks == benchmark.readingZioSchema)
      }
    )

}
