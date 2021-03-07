package zio.dynamodb

import zio.dynamodb.DynamoDBQuery._
import zio.{ Chunk, Has, ZIO, ZLayer }

import scala.collection.immutable.{ Map => ScalaMap }

object DynamoDBExecutor {
  type DynamoDBExecutor = Has[Service]

  trait Service {
    def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A]
  }

  /*
  QUESTIONS
  - what about BatchItem aggregation?
   */
  def parallelize[A](query: DynamoDBQuery[A]): (Chunk[Constructor[Any]], Chunk[Any] => A) =
    query match {
      case Map(query, mapper) =>
        parallelize(query) match {
          case (constructors, assembler) =>
            (constructors, assembler.andThen(mapper))
        }

      case Zip(left, right)   =>
        val (constructorsLeft, assemblerLeft)   = parallelize(left)
        val (constructorsRight, assemblerRight) = parallelize(right)
        (
          constructorsLeft ++ constructorsRight,
          (results: Chunk[Any]) => {
            val (leftResults, rightResults) = results.splitAt(constructorsLeft.length)
            val left                        = assemblerLeft(leftResults)
            val right                       = assemblerRight(rightResults)
            (left, right).asInstanceOf[A]
          }
        )

      case Succeed(value)     => (Chunk.empty, _ => value.asInstanceOf[A])

      case getItem @ GetItem(_, _, _, _, _)           =>
        (
          Chunk(getItem),
          (results: Chunk[Any]) => {
            println(s"GetItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case putItem @ PutItem(_, _, _, _, _, _)        =>
        (
          Chunk(putItem),
          (results: Chunk[Any]) => {
            println(s"PutItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case deleteItem @ DeleteItem(_, _, _, _, _, _)  =>
        (
          Chunk(deleteItem),
          (results: Chunk[Any]) => {
            println(s"DeleteItem results=$results")
            results.head.asInstanceOf[A]
          }
        )

      case scanItem @ Scan(_, _, _, _, _, _, _, _, _) =>
        (
          Chunk(scanItem),
          (results: Chunk[Any]) => {
            println(s"Scan results=$results")
            results.head.asInstanceOf[A]
          }
        )

      // TODO: put, delete
      // TODO: scan, query

      case _                                          =>
        (Chunk.empty, _ => ().asInstanceOf[A]) //TODO: remove
    }

  def live =
    ZLayer.fromService[DynamoDb.Service, DynamoDBExecutor.Service](dynamoDb =>
      new Service {
        override def execute[A](query: DynamoDBQuery[A]): ZIO[Any, Exception, A] = {
//          val queries = loop2(query, List.empty)
//
//          // for now we do not aggregate/batch queries
//          val x = ZIO.foreachPar(queries)(dynamoDb.executeQueryAgainstDdb)
          println(dynamoDb)
          ZIO.succeed(().asInstanceOf[A]) // TODO
        }
      }
    )

  def executeQueryAgainstDdb[A](atomicQuery: DynamoDBQuery[A]): ZIO[Any, Exception, A] =
    atomicQuery match {
      // TODO: figure out how to do the mapping
      case Map(query, mapper)                                                                   =>
        println(s"$query $mapper")
        ZIO.succeed(().asInstanceOf[A]) //.map(mapper)

      case BatchGetItem(requestItems, capacity)                                                 =>
        println(s"$requestItems $capacity")
        ZIO.succeed(BatchGetItem.Response(ScalaMap.empty, ScalaMap.empty))

      case BatchWriteItem(requestItems, capacity, metrics)                                      =>
        println(s"$requestItems $capacity $metrics")
        ZIO.succeed(BatchWriteItem.Response(ScalaMap.empty, null))

      case GetItem(key, tableName, readConsistency, projections, capacity)                      =>
        println(s"$key $tableName $readConsistency $projections $capacity")
        ZIO.succeed(Some(Item(ScalaMap.empty)))

      case PutItem(tableName, item, conditionExpression, capacity, itemMetrics, returnValues)   =>
        println(s"$tableName $item $conditionExpression $capacity $itemMetrics $returnValues")
        ZIO.succeed(())

      case DeleteItem(tableName, key, conditionExpression, capacity, itemMetrics, returnValues) =>
        println(s"$tableName $key $conditionExpression $capacity $itemMetrics $returnValues")
        ZIO.succeed(())

      case x                                                                                    =>
        ZIO.fail(new Exception(s"$x not implemented yet"))
    }

}
//object DynamoDBExecutorExample extends App {
//  val getItem1                                                = GetItem(key = PrimaryKey(ScalaMap.empty), tableName = TableName("T1"))
//  val getItem2                                                = GetItem(key = PrimaryKey(ScalaMap.empty), tableName = TableName("T2"))
//  val zippedGets: DynamoDBQuery[(Option[Item], Option[Item])] = getItem1 zip getItem2
//
//  val putItem                                   = PutItem(tableName = TableName("T1"), item = Item(ScalaMap.empty))
//  val deleteItem                                = DeleteItem(tableName = TableName("T2"), key = PrimaryKey(ScalaMap.empty))
//  val zippedWrites: DynamoDBQuery[(Unit, Unit)] = putItem zip deleteItem
//
//  val mixed: DynamoDBQuery[((Option[Item], Option[Item]), (Unit, Unit))] =
//    zippedGets zip zippedWrites // should we even allow this?
//
//  val queries: List[DynamoDBQuery[Any]] = DynamoDBExecutor.loop2(mixed, List.empty)
//
//  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
//    val x: ZIO[Any, Exception, List[Any]] = ZIO.foreachPar(queries)(DynamoDBExecutor.executeQueryAgainstDdb)
//    x.map(_.foreach(x => println(s"x=$x"))).exitCode
//  }
//}
