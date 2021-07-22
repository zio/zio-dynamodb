package zio.dynamodb

package object fake {
  type TableEntry = (PrimaryKey, Item)
}
