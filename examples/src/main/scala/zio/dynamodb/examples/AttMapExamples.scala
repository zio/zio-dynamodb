package zio.dynamodb.examples

import zio.dynamodb.{ Item, PrimaryKey }

object AttMapExamples {

  // Note AttrMap has PrimaryKey and Item aliases to aid readability of queries

  val primaryKey         = PrimaryKey("field1" -> 1.0, "field2" -> "X", "field3" -> true, "field4" -> null)
  val itemSimple         = Item("field1" -> 1, "field2" -> "X", "field3" -> true)
  val itemNested         = Item("field1" -> 1, "field2" -> "X", "field3" -> Item("field4" -> 1, "field5" -> "X"))
  val itemEvenMoreNested = Item(
    "field1" -> 1,
    "field2" -> "X",
    "field3" -> Item("field4" -> 1, "field5" -> Item("field6" -> 1, "field7" -> "X"))
  )
}
