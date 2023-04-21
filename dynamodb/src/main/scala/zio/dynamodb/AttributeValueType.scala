package zio.dynamodb

sealed trait AttributeValueType { self =>
  def render: AliasMapRender[String]   =
    self match {
      case valueType: PrimitiveValueType => valueType.render
      case AttributeValueType.Bool       => AliasMapRender.getOrInsert(AttributeValue.String("BOOL"))
      case AttributeValueType.BinarySet  => AliasMapRender.getOrInsert(AttributeValue.String("BS"))
      case AttributeValueType.List       => AliasMapRender.getOrInsert(AttributeValue.String("L"))
      case AttributeValueType.Map        => AliasMapRender.getOrInsert(AttributeValue.String("M"))
      case AttributeValueType.NumberSet  => AliasMapRender.getOrInsert(AttributeValue.String("NS"))
      case AttributeValueType.Null       => AliasMapRender.getOrInsert(AttributeValue.String("NULL"))
      case AttributeValueType.StringSet  => AliasMapRender.getOrInsert(AttributeValue.String("SS"))
    }
  def render2: AliasMapRender2[String] =
    self match {
      case valueType: PrimitiveValueType => valueType.render2
      case AttributeValueType.Bool       => AliasMapRender2.getOrInsert(AttributeValue.String("BOOL"))
      case AttributeValueType.BinarySet  => AliasMapRender2.getOrInsert(AttributeValue.String("BS"))
      case AttributeValueType.List       => AliasMapRender2.getOrInsert(AttributeValue.String("L"))
      case AttributeValueType.Map        => AliasMapRender2.getOrInsert(AttributeValue.String("M"))
      case AttributeValueType.NumberSet  => AliasMapRender2.getOrInsert(AttributeValue.String("NS"))
      case AttributeValueType.Null       => AliasMapRender2.getOrInsert(AttributeValue.String("NULL"))
      case AttributeValueType.StringSet  => AliasMapRender2.getOrInsert(AttributeValue.String("SS"))
    }
}
sealed trait PrimitiveValueType extends AttributeValueType { self =>
  override def render: AliasMapRender[String]   =
    self match {
      case AttributeValueType.Binary => AliasMapRender.getOrInsert(AttributeValue.String("B"))
      case AttributeValueType.Number => AliasMapRender.getOrInsert(AttributeValue.String("N"))
      case AttributeValueType.String => AliasMapRender.getOrInsert(AttributeValue.String("S"))
    }
  override def render2: AliasMapRender2[String] =
    self match {
      case AttributeValueType.Binary => AliasMapRender2.getOrInsert(AttributeValue.String("B"))
      case AttributeValueType.Number => AliasMapRender2.getOrInsert(AttributeValue.String("N"))
      case AttributeValueType.String => AliasMapRender2.getOrInsert(AttributeValue.String("S"))
    }
}

object AttributeValueType {
  // primitive types
  case object Binary    extends PrimitiveValueType
  case object Number    extends PrimitiveValueType
  case object String    extends PrimitiveValueType
  // non primitive types
  case object Bool      extends AttributeValueType
  case object BinarySet extends AttributeValueType
  case object List      extends AttributeValueType
  case object Map       extends AttributeValueType
  case object NumberSet extends AttributeValueType
  case object Null      extends AttributeValueType
  case object StringSet extends AttributeValueType
}
