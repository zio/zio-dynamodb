package zio.dynamodb

import java.io.IOException

import zio.{ Chunk, Task, ZLayer }
import zio.blocking.Blocking

/*
~~GetItem~~
WriteItem
UpdateItem
Scan
PutItem
Query
ListTables
DescribeTable
 */

final case class PrimaryKey(value: Map[String, AttributeValue])

final case class TableName(value: String)

sealed trait ConsistencyMode
object ConsistencyMode {
  case object Strong extends ConsistencyMode
  case object Weak   extends ConsistencyMode
}

sealed trait AttributeValue
object AttributeValue {
  import Predef.{ String => ScalaString, Map => ScalaMap }
  final case class Binary(value: Chunk[Byte])                   extends AttributeValue
  final case class Bool(value: Boolean)                         extends AttributeValue
  final case class BinarySet(value: Chunk[Chunk[Byte]])         extends AttributeValue
  final case class List(value: Chunk[AttributeValue])           extends AttributeValue
  final case class Map(value: ScalaMap[String, AttributeValue]) extends AttributeValue
  final case class Number(value: BigDecimal)                    extends AttributeValue
  final case class NumberSet(value: Set[BigDecimal])            extends AttributeValue
  object Null                                                   extends AttributeValue
  final case class String(value: ScalaString)                   extends AttributeValue
  final case class StringSet(value: Set[ScalaString])           extends AttributeValue
}

// The maximum depth for a document path is 32
sealed trait ProjectionExpression { self =>
  def apply(index: Int): ProjectionExpression = ProjectionExpression.ListElement(self, index)

  def apply(key: String): ProjectionExpression = ProjectionExpression.MapElement(self, key)
}
object ProjectionExpression       {
  def apply(name: String): ProjectionExpression = TopLevel(name)

  final case class TopLevel(name: String)                                extends ProjectionExpression
  final case class MapElement(parent: ProjectionExpression, key: String) extends ProjectionExpression
  final case class ListElement(parent: ProjectionExpression, index: Int) extends ProjectionExpression
}

sealed trait ReturnConsumedCapacity
object ReturnConsumedCapacity {
  case object Indexes extends ReturnConsumedCapacity
  case object Total   extends ReturnConsumedCapacity
  case object None    extends ReturnConsumedCapacity
}

sealed trait DynamoDBQuery[+A] { self =>
  final def <*[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = zipLeft(that)

  final def *>[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = zipRight(that)

  final def <*>[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = self zip that

  final def map[B](f: A => B): DynamoDBQuery[B] = DynamicDBQuery.Map(self, f)

  final def zip[B](that: DynamoDBQuery[B]): DynamoDBQuery[(A, B)] = DynamicDBQuery.Zip(self, that)

  final def zipLeft[B](that: DynamoDBQuery[B]): DynamoDBQuery[A] = (self zip that).map(_._1)

  final def zipRight[B](that: DynamoDBQuery[B]): DynamoDBQuery[B] = (self zip that).map(_._2)
}
object DynamicDBQuery          {
  final case class Succeed[A](value: () => A)                                 extends DynamoDBQuery[A]
  final case class GetItem(
    key: PrimaryKey,
    tableName: TableName,
    readConsistency: ConsistencyMode,
    projections: List[ProjectionExpression],
    capacity: ReturnConsumedCapacity
  )                                                                           extends DynamoDBQuery[Chunk[Byte]]
  final case class Zip[A, B](left: DynamoDBQuery[A], right: DynamoDBQuery[B]) extends DynamoDBQuery[(A, B)]
  final case class Map[A, B](query: DynamoDBQuery[A], mapper: A => B)         extends DynamoDBQuery[B]

  def apply[A](a: => A): DynamoDBQuery[A] = Succeed(() => a)
}

trait DynamoDBTable {
  def get(
    key: PrimaryKey,
    readConsistency: ConsistencyMode = ConsistencyMode.Weak,
    capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
  )(ps: ProjectionExpression*): DynamoDBQuery[Chunk[Byte]]

  def getAll(
    key: PrimaryKey
  ): DynamoDBQuery[Chunk[Byte]] = get(key)()
}

trait DynamoDBQueryFunctions {
  def getTable(tableName: TableName): DynamoDBTable =
    new DynamoDBTable {
      def get(
        key: PrimaryKey,
        readConsistency: ConsistencyMode = ConsistencyMode.Weak,
        capacity: ReturnConsumedCapacity = ReturnConsumedCapacity.None
      )(projections: ProjectionExpression*): DynamoDBQuery[Chunk[Byte]] =
        DynamicDBQuery.GetItem(key, tableName, readConsistency, projections.toList, capacity)

    }
}

trait DynamoDBExecutor  {
  def execute[A](query: DynamoDBQuery[A]): Task[A]
}
object DynamoDBExecutor {
  // TODO: Depend on `sttp`
  def live(): ZLayer[Blocking, IOException, DynamoDBExecutor] = ???
}
