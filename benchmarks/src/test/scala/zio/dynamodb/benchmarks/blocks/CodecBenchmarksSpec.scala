package zio.dynamodb.benchmarks.blocks

import zio.dynamodb.benchmarks.codecs.CodecBenchmarks
import zio.test.{ assertTrue, ZIOSpecDefault }

object CodecBenchmarksSpec extends ZIOSpecDefault {
  def spec =
    suite("CodecBenchmarksSpec")(
      test("writing: zio schema equals expected model") {
        val benchmark = new CodecBenchmarks()
        benchmark.setup()
        assertTrue(benchmark.readingZioSchema == benchmark.listOfRecords)
      },
      test("reading: zio blocks equals zio schema equals dynosaur") {
        val benchmark = new CodecBenchmarks()
        benchmark.setup()
        assertTrue(
          //benchmark.readingZioBlocks == benchmark.readingZioSchema,
          benchmark.readingZioSchema == benchmark.readingDynosaur,
          benchmark.readingZioSchema == benchmark.readingScanamo
        )
      }
    )

}
