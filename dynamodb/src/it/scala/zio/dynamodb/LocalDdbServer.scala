package zio.dynamodb

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import zio.ZIO.attemptBlocking
import zio.{ ZIO, ZLayer }

object LocalDdbServer {

  val inMemoryLayer: ZLayer[Any, Nothing, DynamoDBProxyServer] = {

    val effect = ZIO.acquireRelease(
      ZIO.debug("open in memory layer") *> attemptBlocking {
        System.setProperty(
          "sqlite4java.library.path",
          "dynamodb/native-libs"
        )
        System.setProperty("aws.accessKeyId", "dummy-key")
        System.setProperty("aws.secretKey", "dummy-key")
        System.setProperty("aws.secretAccessKey", "dummy-key")

        // Create an in-memory and in-process instance of DynamoDB Local that runs over HTTP

        val localArgs                   = Array("-inMemory")
        val server: DynamoDBProxyServer = ServerRunner.createServerFromCommandLineArgs(localArgs)
        server.start()
        server
      }.orDie
    )(server => ZIO.debug("close in memory layer") *> attemptBlocking(server.stop()).orDie)

    ZLayer.scoped(effect)
  }
}
