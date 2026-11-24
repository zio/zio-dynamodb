package zio.dynamodb

/** The key attributes of a table or an index: the name of the hash (partition) key and, optionally, of the sort key. */
final case class KeySchema private (hashKey: String, sortKey: Option[String])

/** Creates a [[KeySchema]] from the key names. */
object KeySchema {

  /** Returns a [[KeySchema]] that has only a hash key. */
  def apply(hashKey: String): KeySchema = KeySchema(hashKey, sortKey = None)

  /** Returns a [[KeySchema]] that has a hash key and a sort key. */
  def apply(hashKey: String, sortKey: String): KeySchema = KeySchema(hashKey = hashKey, Some(sortKey))
}
