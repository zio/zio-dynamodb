package zio.dynamodb

import zio.dynamodb.SSESpecification.SSEType

/**
 * The server-side encryption settings of a table. `enable` defaults to `false` and `kmsMasterKeyId` to `None`, while
 * `sseType` has no default.
 */
final case class SSESpecification(
  enable: Boolean = false,
  kmsMasterKeyId: Option[String] = None,
  sseType: SSEType
)

/** The types of server-side encryption. */
object SSESpecification {

  /** A type of server-side encryption, [[SSESpecification.AES256]] or [[SSESpecification.KMS]]. */
  sealed trait SSEType

  /** The `AES256` server-side encryption type. */
  case object AES256 extends SSEType

  /** The `KMS` server-side encryption type, which uses the AWS Key Management Service. */
  case object KMS extends SSEType
}
