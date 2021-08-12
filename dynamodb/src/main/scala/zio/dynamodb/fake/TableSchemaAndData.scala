package zio.dynamodb.fake

private[fake] final case class TableSchemaAndData(tableName: String, pkName: String, entries: List[TableEntry])
