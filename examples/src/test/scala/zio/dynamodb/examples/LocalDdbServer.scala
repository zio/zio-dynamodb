package zio.dynamodb.examples

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer
import zio.{ Has, ZLayer, ZManaged }
import zio.blocking.{ effectBlocking, Blocking }

object LocalDdbServer {

  val inMemoryLayer: ZLayer[Blocking, Nothing, Has[DynamoDBProxyServer]] =
    ZManaged.make {
      effectBlocking {
        System.setProperty("sqlite4java.library.path", "native-libs")
        System.setProperty("aws.accessKeyId", "dummy-key")
        System.setProperty("aws.secretKey", "dummy-key")
        System.setProperty("aws.secretAccessKey", "dummy-key")

        // Create an in-memory and in-process instance of DynamoDB Local that runs over HTTP

        val localArgs                   = Array("-inMemory")
        val server: DynamoDBProxyServer = ServerRunner.createServerFromCommandLineArgs(localArgs)
        server.start()
        server
      }.orDie
    }(server => effectBlocking(server.stop()).orDie).toLayer
}
