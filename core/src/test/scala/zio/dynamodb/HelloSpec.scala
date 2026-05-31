package zio.dynamodb

import zio.test._

object HelloSpec extends ZIOSpecDefault {
  def spec = suite("HelloSpec")(
    test("greet returns a greeting") {
      assertTrue(Hello.greet("World") == "Hello, World!")
    }
  )
}
