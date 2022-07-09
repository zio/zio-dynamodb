package zio.dynamodb.codec

import zio.test.{ assertCompletes, DefaultRunnableSpec, ZSpec }

object ReifiedOpticsWithAnnotationsSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("")(test("first") {
      // foo
      assertCompletes
    })
}
