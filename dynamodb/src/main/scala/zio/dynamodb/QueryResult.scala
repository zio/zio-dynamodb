package zio.dynamodb

import zio.stream.ZStream

// attempts to model 2 modes of getting stuff back
// 1) client does not control paging so we can return a ZStream
// 2) client controls paging via ExclusiveStartKey and limit so we return a list, together with the LastEvaluatedKey
sealed trait QueryResult[-R, +E]
object QueryResult {
  final case class Items[R, E](items: ZStream[R, E, Item]) extends QueryResult[R, E]

  // returned when limit is supplied
  final case class PagedResult(items: List[Item], lastEvaluatedKey: Option[PrimaryKey]) // assumes limit is used
      extends QueryResult[Nothing, Nothing]
}
