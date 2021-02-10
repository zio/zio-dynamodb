package zio.dynamodb

sealed trait Select
case object AllAttributes          extends Select
case object AllProjectedAttributes extends Select
case object SpecificAttributes     extends Select
case object Count                  extends Select
