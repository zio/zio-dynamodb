package zio.dynamodb.benchmarks.blocks

import zio.test.{ assertTrue, ZIOSpecDefault }

object RegistersSpec extends ZIOSpecDefault {
  override def spec =
    suite("RegistersSpec")(
      test("placeholder test") {
        val benchmark = new RegistersBenchmark()
        benchmark.setup()

        benchmark.encodeUsingRegisters()
        benchmark.encodeUsingCachedRegisters()

        assertTrue(true)
      }
    )

}
