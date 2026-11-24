package zio.dynamodb

/**
 * The attributes a query or scan returns. DynamoDB accepts only `SpecificAttributes` when a projection expression is
 * given. The `scanSomeItem`, `scanAllItem`, `querySomeItem` and `queryAllItem` builders choose `SpecificAttributes`
 * when they get projections and `AllAttributes` when they do not.
 */
sealed trait Select

object Select {

  /** Returns all the attributes of the items. */
  case object AllAttributes extends Select

  /** Returns the attributes that are projected into the index. It applies to a read of a secondary index. */
  case object AllProjectedAttributes extends Select

  /** Returns only the attributes named in the projection expression. */
  case object SpecificAttributes extends Select

  /** Returns the number of matching items instead of the items. */
  case object Count extends Select
}
