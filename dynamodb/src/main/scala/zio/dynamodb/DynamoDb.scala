package zio.dynamodb

import zio.{ Has, ZIO, ZLayer }

object DynamoDb {
  type DynamoDb = Has[Service]

  trait Service {
    def executeQueryAgainstDdb[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  def test[A](returnVal: A) =
    ZLayer.succeed(new Service {
      override def executeQueryAgainstDdb[A2](atomicQuery: DynamoDBQuery[A2]): ZIO[Any, Exception, A2] =
        ZIO.succeed(returnVal.asInstanceOf[A2])
    })

}
