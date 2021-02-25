package zio.dynamodb

import zio.dynamodb.SSESpecification.SSEType

final case class SSESpecification(
  enable: Boolean = false,
  kmsMasterKeyId: Option[String] = None,
  sseType: SSEType
)
object SSESpecification {
  sealed trait SSEType
  final case object AES256 extends SSEType
  final case object KMS    extends SSEType
}
