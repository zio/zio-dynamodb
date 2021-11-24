package zio.dynamodb

sealed trait ReturnItemCollectionMetrics

object ReturnItemCollectionMetrics {
  case object None extends ReturnItemCollectionMetrics
  case object Size extends ReturnItemCollectionMetrics

  private[dynamodb] def toZioAws(
    returnItemCollectionMetrics: ReturnItemCollectionMetrics
  ): io.github.vigoo.zioaws.dynamodb.model.ReturnItemCollectionMetrics =
    returnItemCollectionMetrics match {
      case None => io.github.vigoo.zioaws.dynamodb.model.ReturnItemCollectionMetrics.NONE
      case Size => io.github.vigoo.zioaws.dynamodb.model.ReturnItemCollectionMetrics.SIZE
    }
}
