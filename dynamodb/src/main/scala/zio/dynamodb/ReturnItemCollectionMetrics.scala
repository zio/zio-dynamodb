package zio.dynamodb

sealed trait ReturnItemCollectionMetrics

object ReturnItemCollectionMetrics {
  case object None extends ReturnItemCollectionMetrics
  case object Size extends ReturnItemCollectionMetrics

  private[dynamodb] def toZioAws(
    returnItemCollectionMetrics: ReturnItemCollectionMetrics
  ): zio.aws.dynamodb.model.ReturnItemCollectionMetrics =
    returnItemCollectionMetrics match {
      case None => zio.aws.dynamodb.model.ReturnItemCollectionMetrics.NONE
      case Size => zio.aws.dynamodb.model.ReturnItemCollectionMetrics.SIZE
    }
}
