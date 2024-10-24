package zio.dynamodb

final case class KeySchema private (hashKey: String, sortKey: Option[String])
object KeySchema {
  def apply(hashKey: String): KeySchema                  = KeySchema(hashKey, sortKey = None)
  def apply(hashKey: String, sortKey: String): KeySchema = KeySchema(hashKey = hashKey, Some(sortKey))
}
