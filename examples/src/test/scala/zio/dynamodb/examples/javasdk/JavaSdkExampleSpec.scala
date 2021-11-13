package zio.dynamodb.examples.javasdk

import zio.ZIO
import zio.dynamodb.examples.LocalDdbServer
import zio.test.{ assertCompletes, DefaultRunnableSpec }

object JavaSdkExampleSpec extends DefaultRunnableSpec {
  override def spec =
    suite("JavaSdkExample suite")(
      testM("first test") {
        val completableFuture = DdbHelper.dynamoDbAsyncClient.createTable(DdbHelper.createTableRequest)
        val zioCreateTable    = ZIO.fromCompletionStage(completableFuture)

        for {
          _ <- zioCreateTable
        } yield assertCompletes
      }.provideCustomLayer(LocalDdbServer.inMemoryLayer ++ DdbHelper.ddbLayer)
    )

}
