package zio.dynamodb

final case class AliasMap private (map: Map[AttributeValue, String], index: Int = 0) { self =>
  // REVIEW: Is this a reasonable interface?
  private def +(entry: AttributeValue): (AliasMap, String) = {
    val variableAlias = s":v${self.index}"
    (AliasMap(self.map + ((entry, variableAlias)), self.index + 1), variableAlias)
  }

  def getOrInsert(entry: AttributeValue): (AliasMap, String) =
    self.map.get(entry).map(varName => (self, varName)).getOrElse {
      self + entry
    }

  def isEmpty: Boolean = self.index == 0
}

object AliasMap {
  def empty: AliasMap = AliasMap(Map.empty, 0)
}

final case class AliasMapRender[+A](
  render: AliasMap => (AliasMap, A)
) { self =>

  def map[B](f: A => B): AliasMapRender[B] =
    AliasMapRender { aliasMap =>
      val (am, a) = self.render(aliasMap)
      (am, f(a))
    }

  def flatMap[B](f: A => AliasMapRender[B]): AliasMapRender[B] =
    AliasMapRender { aliasMap =>
      val (am, a) = self.render(aliasMap)
      f(a).render(am)
    }

  def zipWith[B, C](that: AliasMapRender[B])(f: (A, B) => C): AliasMapRender[C] =
    for {
      a <- self
      b <- that
    } yield f(a, b)

  def zipRight[B](that: AliasMapRender[B]): AliasMapRender[B] =
    self.flatMap(_ => that)

}

object AliasMapRender {
  def getOrInsert(entry: AttributeValue): AliasMapRender[String] =
    AliasMapRender { aliasMap =>
      aliasMap.getOrInsert(entry)
    }

  def empty = AliasMapRender.setMap(AliasMap.empty)

  def succeed[A](a: => A): AliasMapRender[A] = AliasMapRender(aliasMap => (aliasMap, a))

  val getMap: AliasMapRender[AliasMap] = AliasMapRender(aliasMap => (aliasMap, aliasMap))

  def setMap(aliasMap: AliasMap): AliasMapRender[Unit] =
    AliasMapRender { _ =>
      (aliasMap, ())
    }

}

sealed trait KeyConditionExpression { self =>
  def render: AliasMapRender[String] =
    self match {
      case KeyConditionExpression.And(left, right) =>
        left.render
          .zipWith(
            right.render
          ) { case (l, r) => s"$l AND $r" }
      case expression: PartitionKeyExpression      => expression.render
    }
}

object KeyConditionExpression {
  private[dynamodb] final case class And(left: PartitionKeyExpression, right: SortKeyExpression)
      extends KeyConditionExpression
}

sealed trait PartitionKeyExpression extends KeyConditionExpression { self =>
  import KeyConditionExpression.And

  def &&(that: SortKeyExpression): KeyConditionExpression = And(self, that)

  override def render: AliasMapRender[String] =
    self match {
      case PartitionKeyExpression.Equals(left, right) =>
        AliasMapRender.getOrInsert(right).map(v => s"${left.keyName} = $v")
    }
}
object PartitionKeyExpression {
  final case class PartitionKey(keyName: String) { self =>
    def ===[A](that: A)(implicit t: ToAttributeValue[A]): PartitionKeyExpression =
      Equals(self, t.toAttributeValue(that))
  }
  final case class Equals(left: PartitionKey, right: AttributeValue) extends PartitionKeyExpression
}

sealed trait SortKeyExpression { self =>
  def render: AliasMapRender[String] =
    self match {
      case SortKeyExpression.Equals(left, right)             =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} = $v"
          }
      case SortKeyExpression.LessThan(left, right)           =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} < $v"
          }
      case SortKeyExpression.NotEqual(left, right)           =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} <> $v"
          }
      case SortKeyExpression.GreaterThan(left, right)        =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} > $v"
          }
      case SortKeyExpression.LessThanOrEqual(left, right)    =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} <= $v"
          }
      case SortKeyExpression.GreaterThanOrEqual(left, right) =>
        AliasMapRender
          .getOrInsert(right)
          .map { v =>
            s"${left.keyName} >= $v"
          }
      case SortKeyExpression.Between(left, min, max)         =>
        AliasMapRender
          .getOrInsert(min)
          .flatMap(min =>
            AliasMapRender.getOrInsert(max).map { max =>
              s"${left.keyName} BETWEEN $min AND $max"
            }
          )
      case SortKeyExpression.BeginsWith(left, value)         =>
        AliasMapRender
          .getOrInsert(value)
          .map { v =>
            s"begins_with(${left.keyName}, $v)"
          }
    }
}

object SortKeyExpression {

  final case class SortKey(keyName: String) { self =>
    def ===[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression            = Equals(self, t.toAttributeValue(that))
    def <>[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression             = NotEqual(self, t.toAttributeValue(that))
    def <[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression              = LessThan(self, t.toAttributeValue(that))
    def <=[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression             =
      LessThanOrEqual(self, t.toAttributeValue(that))
    def >[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression              =
      GreaterThanOrEqual(self, t.toAttributeValue(that))
    def >=[A](that: A)(implicit t: ToAttributeValue[A]): SortKeyExpression             =
      GreaterThanOrEqual(self, t.toAttributeValue(that))
    def between[A](min: A, max: A)(implicit t: ToAttributeValue[A]): SortKeyExpression =
      Between(self, t.toAttributeValue(min), t.toAttributeValue(max))
    def beginsWith[A](value: A)(implicit t: ToAttributeValue[A]): SortKeyExpression    =
      BeginsWith(self, t.toAttributeValue(value))
  }

  private[dynamodb] final case class Equals(left: SortKey, right: AttributeValue)             extends SortKeyExpression
  private[dynamodb] final case class NotEqual(left: SortKey, right: AttributeValue)           extends SortKeyExpression
  private[dynamodb] final case class LessThan(left: SortKey, right: AttributeValue)           extends SortKeyExpression
  private[dynamodb] final case class GreaterThan(left: SortKey, right: AttributeValue)        extends SortKeyExpression
  private[dynamodb] final case class LessThanOrEqual(left: SortKey, right: AttributeValue)    extends SortKeyExpression
  private[dynamodb] final case class GreaterThanOrEqual(left: SortKey, right: AttributeValue) extends SortKeyExpression
  private[dynamodb] final case class Between(left: SortKey, min: AttributeValue, max: AttributeValue)
      extends SortKeyExpression
  private[dynamodb] final case class BeginsWith(left: SortKey, value: AttributeValue)         extends SortKeyExpression
}
