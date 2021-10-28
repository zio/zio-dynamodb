package zio.dynamodb

import io.github.vigoo.zioaws.core.config
import io.github.vigoo.zioaws.{ dynamodb, http4s }
import zio.dynamodb.DynamoDBQuery.putItem
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }
import zio.test.Assertion._

object LiveSpec extends DefaultRunnableSpec {

  System.setProperty("sqlite4java.library.path", "lib")
  override def spec: ZSpec[_root_.zio.dynamodb.LiveSpec.Environment, _root_.zio.dynamodb.LiveSpec.Failure] =
    suite("LiveTest")(
      testM("something something") {
        (for {
          _ <- program
        } yield assert(true)(isTrue)).provideCustomLayer(layer)
      }
    )

  private val program =
    putItem("zio-dynamodb-test", Item("id" -> "first", "firstName" -> "adam")).execute

  val layer           = http4s.default >>> config.default >>> dynamodb.live >>> DynamoDBExecutor.live

}
