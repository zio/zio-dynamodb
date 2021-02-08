package zio.dynamodb

sealed trait ReturnItemCollectionMetrics
object ReturnItemCollectionMetrics {
  case object None extends ReturnItemCollectionMetrics
  case object Size extends ReturnItemCollectionMetrics
}
