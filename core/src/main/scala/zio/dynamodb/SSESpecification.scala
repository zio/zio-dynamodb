package zio.dynamodb

import zio.dynamodb.SSESpecification.SSEType

final case class SSESpecification(
  enable: Boolean = false,
  kmsMasterKeyId: Option[String] = None,
  sseType: SSEType
)
object SSESpecification {
  sealed trait SSEType
  case object AES256 extends SSEType
  case object KMS    extends SSEType
}
