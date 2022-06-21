package zio.dynamodb
import zio.Chunk
import zio.test._
import zio.dynamodb.ProjectionExpression._
import zio.test.Assertion._

object AliasMapRenderSpec extends DefaultRunnableSpec {

  val one    = AttributeValue.Number(1)
  val two    = AttributeValue.Number(2)
  val three  = AttributeValue.Number(3)
  val number = AttributeValue.String("N")
  val name   = AttributeValue.String("name")
  val list   = AttributeValue.List(List(one, two, three))

  val between                                               = ConditionExpression
    .Between(
      ConditionExpression.Operand.ValueOperand(two),
      one,
      three
    )
  val in                                                    = ConditionExpression
    .In(
      ConditionExpression.Operand.ValueOperand(one),
      Set(one, two)
    )
  private val projection                                    = "projection"
  private val projectionExpression: ProjectionExpression[_] = $(projection)
  val attributeExists                                       = ConditionExpression.AttributeExists(projectionExpression)
  val attributeNotExists                                    = ConditionExpression.AttributeNotExists(projectionExpression)
  val attributeType                                         = ConditionExpression
    .AttributeType(projectionExpression, AttributeValueType.Number)
  val contains                                              = ConditionExpression
    .Contains(
      projectionExpression,
      one
    )
  val beginsWith                                            = ConditionExpression
    .BeginsWith(
      projectionExpression,
      name
    )

  val setOperandValueOne   = UpdateExpression.SetOperand.ValueOperand(one)
  val setOperandValueTwo   = UpdateExpression.SetOperand.ValueOperand(two)
  val setOperandValueThree = UpdateExpression.SetOperand.ValueOperand(three)

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("AliasMapRender")(
      suite("ConditionExpression")(
        suite("basic renders")(
          test("Between") {
            val (aliasMap, expression) = between.render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0", one -> ":v1", three -> ":v2"), 3))) &&
            assert(expression)(equalTo(":v0 BETWEEN :v1 AND :v2"))
          },
          test("In") {
            val (aliasMap, expression) = in.render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", two -> ":v1"), 2))) &&
            assert(expression)(equalTo(":v0 IN (:v0, :v1)"))
          },
          test("AttributeExists") {
            val (aliasMap, expression) =
              attributeExists.render.execute
            assert(aliasMap.map)(isEmpty) &&
            assert(expression)(equalTo(s"attribute_exists($projection)"))
          },
          test("AttributeNotExists") {
            val (aliasMap, expression) =
              attributeNotExists.render.execute

            assert(aliasMap.map)(isEmpty) &&
            assert(expression)(equalTo(s"attribute_not_exists($projection)"))

          },
          test("AttributeType") {
            val (aliasMap, expression) = attributeType.render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(number -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"attribute_type($projection, :v0)"))
          },
          test("Contains") {
            val (aliasMap, expression) = contains.render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"contains($projection, :v0)"))
          },
          test("BeginsWith") {
            val (aliasMap, expression) = beginsWith.render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(name -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"begins_with($projection, :v0)"))
          },
          test("And") {

            val (aliasMap, expression) = ConditionExpression
              .And(
                between,
                attributeType
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0", one -> ":v1", three -> ":v2", number -> ":v3"), 4))) &&
            assert(expression)(equalTo("(:v0 BETWEEN :v1 AND :v2) AND (attribute_type(projection, :v3))"))
          },
          test("Or") {
            val (aliasMap, expression) = ConditionExpression
              .Or(
                between,
                attributeType
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0", one -> ":v1", three -> ":v2", number -> ":v3"), 4))) &&
            assert(expression)(equalTo("(:v0 BETWEEN :v1 AND :v2) OR (attribute_type(projection, :v3))"))
          },
          test("Not") {
            val (aliasMap, expression) = ConditionExpression
              .Not(beginsWith)
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(name -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"NOT (begins_with($projection, :v0))"))
          },
          test("Equals") {
            val (aliasMap, expression) = ConditionExpression
              .Equals(
                ConditionExpression.Operand.ValueOperand(two),
                ConditionExpression.Operand.Size(projectionExpression)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"(:v0) = (size($projection))"))
          },
          test("Equals with duplicates") {
            val (aliasMap, expression) = ConditionExpression
              .Equals(
                ConditionExpression.Operand.ValueOperand(two),
                ConditionExpression.Operand.ValueOperand(two)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"(:v0) = (:v0)"))
          },
          test("NotEqual") {
            val (aliasMap, expression) = ConditionExpression
              .NotEqual(
                ConditionExpression.Operand.ValueOperand(two),
                ConditionExpression.Operand.Size(projectionExpression)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0"), 1))) &&
            assert(expression)(equalTo(s"(:v0) <> (size($projection))"))
          },
          test("LessThan") {
            val (aliasMap, expression) = ConditionExpression
              .LessThan(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", three -> ":v1"), 2))) &&
            assert(expression)(equalTo("(:v0) < (:v1)"))
          },
          test("GreaterThan") {
            val (aliasMap, expression) = ConditionExpression
              .GreaterThan(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", three -> ":v1"), 2))) &&
            assert(expression)(equalTo("(:v0) > (:v1)"))
          },
          test("LessThanOrEqual") {
            val (aliasMap, expression) = ConditionExpression
              .LessThanOrEqual(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", three -> ":v1"), 2))) &&
            assert(expression)(equalTo("(:v0) <= (:v1)"))
          },
          test("GreaterThanOrEqual") {
            val (aliasMap, expression) = ConditionExpression
              .GreaterThanOrEqual(
                ConditionExpression.Operand.ValueOperand(one),
                ConditionExpression.Operand.ValueOperand(three)
              )
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", three -> ":v1"), 2))) &&
            assert(expression)(equalTo("(:v0) >= (:v1)"))
          }
        )
      ),
      suite("KeyConditionExpression")(
        suite("SortKeyExpression")(
          test("Equals") {
            val (aliasMap, expression) =
              SortKeyExpression.Equals(SortKeyExpression.SortKey("num"), one).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num = :v0"))
          },
          test("LessThan") {
            val (aliasMap, expression) =
              SortKeyExpression.LessThan(SortKeyExpression.SortKey("num"), one).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num < :v0"))
          },
          test("NotEqual") {
            val (aliasMap, expression) =
              SortKeyExpression.NotEqual(SortKeyExpression.SortKey("num"), one).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num <> :v0"))
          },
          test("GreaterThan") {
            val (aliasMap, expression) =
              SortKeyExpression.GreaterThan(SortKeyExpression.SortKey("num"), one).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num > :v0"))
          },
          test("LessThanOrEqual") {
            val (aliasMap, expression) =
              SortKeyExpression.LessThanOrEqual(SortKeyExpression.SortKey("num"), one).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num <= :v0"))
          },
          test("GreaterThanOrEqual") {
            val (aliasMap, expression) =
              SortKeyExpression.GreaterThanOrEqual(SortKeyExpression.SortKey("num"), one).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num >= :v0"))
          },
          test("Between") {
            val (aliasMap, expression) =
              SortKeyExpression.Between(SortKeyExpression.SortKey("num"), one, two).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", two -> ":v1"), 2))) &&
            assert(expression)(equalTo("num BETWEEN :v0 AND :v1"))
          },
          test("BeginsWith") {
            val (aliasMap, expression) =
              SortKeyExpression.BeginsWith(SortKeyExpression.SortKey("num"), name).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(name -> ":v0"), 1))) &&
            assert(expression)(equalTo("begins_with(num, :v0)"))
          }
        ),
        suite("PartitionKeyExpression")(
          test("Equals") {
            val (aliasMap, expression) = PartitionKeyExpression
              .Equals(PartitionKeyExpression.PartitionKey("num"), one)
              .render
              .execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("num = :v0"))
          }
        ),
        test("And") {
          val (aliasMap, expression) = KeyConditionExpression
            .And(
              PartitionKeyExpression
                .Equals(PartitionKeyExpression.PartitionKey("num"), two),
              SortKeyExpression.Between(SortKeyExpression.SortKey("num"), one, three)
            )
            .render
            .execute

          assert(aliasMap)(equalTo(AliasMap(Map(two -> ":v0", one -> ":v1", three -> ":v2"), 3))) &&
          assert(expression)(equalTo("num = :v0 AND num BETWEEN :v1 AND :v2"))
        }
      ),
      suite("AttributeValueType")(
        test("Bool") {
          val (aliasMap, expression) = AttributeValueType.Bool.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("BOOL") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("BinarySet") {
          val (aliasMap, expression) = AttributeValueType.BinarySet.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("BS") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("List") {
          val (aliasMap, expression) = AttributeValueType.List.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("L") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("Map") {
          val (aliasMap, expression) = AttributeValueType.Map.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("M") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("NumberSet") {
          val (aliasMap, expression) = AttributeValueType.NumberSet.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("NS") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("Null") {
          val (aliasMap, expression) = AttributeValueType.Null.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("NULL") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("StringSet") {
          val (aliasMap, expression) = AttributeValueType.StringSet.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("SS") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("Binary") {
          val (aliasMap, expression) = AttributeValueType.Binary.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("B") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("Number") {
          val (aliasMap, expression) = AttributeValueType.Number.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("N") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        },
        test("String") {
          val (aliasMap, expression) = AttributeValueType.String.render.execute

          assert(aliasMap)(equalTo(AliasMap(Map(AttributeValue.String("S") -> ":v0"), 1))) &&
          assert(expression)(equalTo(":v0"))
        }
      ),
      suite("UpdateExpression")(
        suite("multiple actions")(
          test("Set and Remove") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.Actions(
                  Chunk(
                    UpdateExpression.Action.SetAction(
                      $(projection),
                      UpdateExpression.SetOperand.IfNotExists(
                        projectionExpression,
                        one
                      )
                    ),
                    UpdateExpression.Action.RemoveAction($("otherProjection"))
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("set projection = if_not_exists(projection, :v0) remove otherProjection"))
          },
          test("Two Sets") {

            val (aliasMap, expression) =
              (UpdateExpression.Action.SetAction($(projection), UpdateExpression.SetOperand.ValueOperand(one)) +
                UpdateExpression.Action.SetAction(
                  $("otherProjection"),
                  UpdateExpression.SetOperand.ValueOperand(one)
                ) + UpdateExpression.Action.AddAction($("lastProjection"), one)).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("set projection = :v0,otherProjection = :v0 add lastProjection :v0"))
          }
        ),
        suite("Set")(
          test("Minus") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $(projection),
                  UpdateExpression.SetOperand.Minus(
                    setOperandValueOne,
                    setOperandValueTwo
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", two -> ":v1"), 2))) &&
            assert(expression)(equalTo("set projection = :v0 - :v1"))
          },
          test("Plus") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $(projection),
                  UpdateExpression.SetOperand.Plus(
                    setOperandValueOne,
                    setOperandValueTwo
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0", two -> ":v1"), 2))) &&
            assert(expression)(equalTo("set projection = :v0 + :v1"))

          },
          test("ValueOperand") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $(projection),
                  UpdateExpression.SetOperand.ValueOperand(one)
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("set projection = :v0"))

          },
          test("PathOperand") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $(projection),
                  UpdateExpression.SetOperand.PathOperand(
                    projectionExpression
                  )
                )
              ).render.execute

            assert(aliasMap.map)(isEmpty) &&
            assert(expression)(equalTo("set projection = projection"))

          },
          test("ListAppend") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $(projection),
                  UpdateExpression.SetOperand.ListAppend(
                    projectionExpression,
                    list
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(list -> ":v0"), 1))) &&
            assert(expression)(equalTo("set projection = list_append(projection, :v0)"))

          },
          test("ListPrepend") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $(projection),
                  UpdateExpression.SetOperand.ListPrepend(
                    projectionExpression,
                    list
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(list -> ":v0"), 1))) &&
            assert(expression)(equalTo("set projection = list_append(:v0, projection)"))

          },
          test("IfNotExists") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.SetAction(
                  $(projection),
                  UpdateExpression.SetOperand.IfNotExists(
                    projectionExpression,
                    one
                  )
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("set projection = if_not_exists(projection, :v0)"))

          }
        ),
        suite("Remove")(
          test("Remove") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.RemoveAction(
                  $(projection)
                )
              ).render.execute

            assert(aliasMap.map)(isEmpty) &&
            assert(expression)(equalTo("remove projection"))
          }
        ),
        suite("Add")(
          test("Add") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.AddAction(
                  $(projection),
                  one
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("add projection :v0"))
          }
        ),
        suite("Delete")(
          test("Delete") {
            val (aliasMap, expression) =
              UpdateExpression(
                UpdateExpression.Action.DeleteAction(
                  $(projection),
                  one
                )
              ).render.execute

            assert(aliasMap)(equalTo(AliasMap(Map(one -> ":v0"), 1))) &&
            assert(expression)(equalTo("delete projection :v0"))
          }
        )
      )
    )

}
