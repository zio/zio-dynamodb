package zio.dynamodb

/**
 * Whether a write returns statistics about the item collection it modified. The values map to the DynamoDB values
 * `NONE` and `SIZE`.
 */
sealed trait ReturnItemCollectionMetrics

object ReturnItemCollectionMetrics {

  /** Returns no statistics (`NONE`). */
  case object None extends ReturnItemCollectionMetrics

  /** Returns statistics that include the size estimate of the item collection (`SIZE`). */
  case object Size extends ReturnItemCollectionMetrics

  private[dynamodb] def toZioAws(
    returnItemCollectionMetrics: ReturnItemCollectionMetrics
  ): zio.aws.dynamodb.model.ReturnItemCollectionMetrics =
    returnItemCollectionMetrics match {
      case None => zio.aws.dynamodb.model.ReturnItemCollectionMetrics.NONE
      case Size => zio.aws.dynamodb.model.ReturnItemCollectionMetrics.SIZE
    }
}
