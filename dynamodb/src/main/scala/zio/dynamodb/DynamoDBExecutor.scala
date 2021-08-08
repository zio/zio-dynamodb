package zio.dynamodb

import zio.{ Has, ZIO }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

}
