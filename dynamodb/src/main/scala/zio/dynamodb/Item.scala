package zio.dynamodb

// TODO: could we unify this concept with PrimaryKey as they have the same structure?
final case class Item(value: Map[String, AttributeValue])
