package zio.dynamodb

import zio._
import zio.test.{ assert, DefaultRunnableSpec, ZSpec }
import zio.test.Assertion._

object LiveSpec extends DefaultRunnableSpec {

  System.setProperty("sqlite4java.library.path", "lib")
  override def spec: ZSpec[_root_.zio.dynamodb.LiveSpec.Environment, _root_.zio.dynamodb.LiveSpec.Failure] =
    suite("LiveTest")(
      testM("something something") {
        for {
          a <- ZIO.succeed(true)
        } yield assert(a)(isTrue)
      }
    )

//  def something() =
//    ZManaged.fromEffect(DynamoDBLocal.randomPort)

}
