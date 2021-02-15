package zio.dynamodb

// if ProjectExpression supplied then only valid value is SpecificAttributes
sealed trait Select

object Select {
  case object AllAttributes          extends Select
  case object AllProjectedAttributes extends Select
  case object SpecificAttributes     extends Select
  case object Count                  extends Select
}
