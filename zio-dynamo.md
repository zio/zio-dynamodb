# zio-dynamodb

renames for Codec -> DynamoDbCodec or put in dynamodb package object

- layering insights
  - dynamoDB layer now has a concrete model - library assumes FDM used - case classes and traits
  - other layers like service/core layer will transform dynamoDB model  
- enum constValue addition
- added zio-schema native set implementation
- added zio-dynamodb native set implementation



































## enums codec strategies

```scala
  sealed trait OneOf
  final case class StringValue(value: String)   extends OneOf
  final case class IntValue(value: Int)         extends OneOf
  final case class BooleanValue(value: Boolean) extends OneOf

  final case class CaseClassOne(oneOf: OneOf)
```
Current encoding introduces an intermediate map so that it's key encodes the enum Type eg `StringValue` below

`Item("oneOf" -> Map("StringValue" -> Map("value" -> "one")))`

My hunch is that most teams will currently be packing the Type information as just another field but with special meaning in the Map
ie a Discriminator Field eg

`Item("oneOf" -> Map("value" -> "one", "discriminator" -> "StringValue"))`

This maybe better default behaviour than what we have ATM
If we go with approach we probably would need a config mechanism for overriding the discriminator name

## Notes

could have a discrimiator annotation
case class discriminator(name: String) extends scala.annotation.Annotation
@discriminator("myDiscriminator") // by default uses map encoding


have a guarantee that all cases in an enumeration are records or case objects

check decoder handles case objects

1) pass discriminator name to function 















PUTITEM RESPONSE SYNTAX

```json
{
   "Attributes": { 
      "string" : { 
         "B": blob,
         "BOOL": boolean,
         "BS": [ blob ],
         "L": [ 
            "AttributeValue"
         ],
         "M": { 
            "string" : "AttributeValue"
         },
         "N": "string",
         "NS": [ "string" ],
         "NULL": boolean,
         "S": "string",
         "SS": [ "string" ]
      }
   },
   "ConsumedCapacity": { 
      "CapacityUnits": number,
      "GlobalSecondaryIndexes": { 
         "string" : { 
            "CapacityUnits": number,
            "ReadCapacityUnits": number,
            "WriteCapacityUnits": number
         }
      },
      "LocalSecondaryIndexes": { 
         "string" : { 
            "CapacityUnits": number,
            "ReadCapacityUnits": number,
            "WriteCapacityUnits": number
         }
      },
      "ReadCapacityUnits": number,
      "Table": { 
         "CapacityUnits": number,
         "ReadCapacityUnits": number,
         "WriteCapacityUnits": number
      },
      "TableName": "string",
      "WriteCapacityUnits": number
   },
   "ItemCollectionMetrics": { 
      "ItemCollectionKey": { 
         "string" : { 
            "B": blob,
            "BOOL": boolean,
            "BS": [ blob ],
            "L": [ 
               "AttributeValue"
            ],
            "M": { 
               "string" : "AttributeValue"
            },
            "N": "string",
            "NS": [ "string" ],
            "NULL": boolean,
            "S": "string",
            "SS": [ "string" ]
         }
      },
      "SizeEstimateRangeGB": [ number ]
   }
}
```




```scala
package zio.dynamodb

sealed trait Zippable[-A, -B] {
  type Out

  def zip(left: A, right: B): Out
}
object Zippable extends ZippableLowPriority {
  type Out[-A, -B, C] = Zippable[A, B] { type Out = C }

  implicit def Zippable3[A, B, Z]: Zippable.Out[(A, B), Z, (A, B, Z)] =
    new Zippable[(A, B), Z] {
      type Out = (A, B, Z)

      def zip(left: (A, B), right: Z): Out = (left._1, left._2, right)
    }

  implicit def Zippable4[A, B, C, Z]: Zippable.Out[(A, B, C), Z, (A, B, C, Z)] =
    new Zippable[(A, B, C), Z] {
      type Out = (A, B, C, Z)

      def zip(left: (A, B, C), right: Z): Out = (left._1, left._2, left._3, right)
    }

  implicit def Zippable5[A, B, C, D, Z]: Zippable.Out[(A, B, C, D), Z, (A, B, C, D, Z)] =
    new Zippable[(A, B, C, D), Z] {
      type Out = (A, B, C, D, Z)

      def zip(left: (A, B, C, D), right: Z): Out = (left._1, left._2, left._3, left._4, right)
    }
}
trait ZippableLowPriority {
  implicit def Zippable2[A, B]: Zippable.Out[A, B, (A, B)] =
    new Zippable[A, B] {
      type Out = (A, B)

      def zip(left: A, right: B): Out = (left, right)
    }
}
```
