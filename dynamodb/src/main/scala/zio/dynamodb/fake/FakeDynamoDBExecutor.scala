package zio.dynamodb.fake

object FakeDynamoDBExecutor {

  /**
   * A Fake implementation of `DynamoDBExecutor.Service` with the very modest aspiration of providing bare minimum
   * functionality to enable internal unit tests and to enable simple end to end examples that can serve as documentation.
   * Limited CRUD functionality is supported hence some features are currently not supported or have restrictions.
   *  - Supported
   *    - CRUD operations GetItem, PutItem, DeleteItem, BatchGetItem, BatchWriteItem
   *  - Limited support
   *    - Primary Keys - only one primary key can be specified and it can have only one attribute which is only checked for equality
   *  - Not currently supported
   *    - Expressions - these include Condition Expressions, Projection Expressions, UpdateExpressions, Filter Expressions
   *    - Create table, Delete table
   *    - UpdateItem - this is a more complex case as it uses an expression to specify the update
   *
   * '''Usage''': The schema has to be predefined using a builder style `table` method to specify a table, a single primary
   * and a var arg list of primary key/item pairs. Finally the `layer` method is used to return the layer.
   * {{{
   * testM("getItem") {
   *   for {
   *     result  <- GetItem(key = primaryKey1, tableName = tableName1).execute
   *     expected = Some(item1)
   *   } yield assert(result)(equalTo(expected))
   * }.provideLayer(FakeDynamoDBExecutor
   *   .table("tableName1", pkFieldName = "k1")(primaryKey1 -> item1, primaryKey1_2 -> item1_2)
   *   .table("tableName3", pkFieldName = "k3")(primaryKey3 -> item3)))
   *   .layer
   * }}}
   * @param db
   */
  def table(tableName: String, pkFieldName: String)(entries: TableEntry*): FakeDynamoDBExecutorBuilder =
    FakeDynamoDBExecutorBuilder().table(tableName, pkFieldName)(entries: _*)

}
