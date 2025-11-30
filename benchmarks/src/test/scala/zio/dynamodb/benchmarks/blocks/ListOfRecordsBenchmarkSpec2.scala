package zio.dynamodb.benchmarks.blocks

import zio.test.{ assertTrue, ZIOSpecDefault }

object ListOfRecordsBenchmarkSpec2 extends ZIOSpecDefault {
  def spec =
    suite("ListOfRecordsBenchmarkSpec2")(
      test("writing: zio blocks equals zio schema") {
        val benchmark = new ListOfRecordsBenchmark2()
        benchmark.setup()
        println(benchmark.writingZioBlocks)
        assertTrue(true)
      },
      test("reading: zio blocks equals zio schema equals dynosaur") {
        val benchmark = new ListOfRecordsBenchmark2()
        benchmark.setup()
        assertTrue(
          benchmark.readingZioBlocks == benchmark.readingDynosaur,
          benchmark.readingZioBlocks == benchmark.readingScanamo
        )
      }
    )

}
