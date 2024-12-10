---
id: primary-keys
title: "Primary Keys"
---

In the native AWS DynamoDB API primary keys are represented in two different ways depending on context:

 
| AWS|Example|Context
| ---|---|---
| Primary Keys | `{"id": "1", "year": 2023}` | `GetItem`, `PutItem`, `DeleteItem` 
| Key Condition Expressions | `#id=:val1 and #year > :val2` | `Query`

## Unified Type Safe High Level API for Primary Key Expressions
Assuming the below model
```scala
final case class Person(id: String, year: Int, address: String)
object Person {
  implicit val schema: Schema.CaseClass3[String, Int, String, Person] = DeriveSchema.gen[Person]
  val (id, year, address) = ProjectExpression.accessors[Person]
}
```

The High Level API unifies the two different ways into a single Type Safe API that is accessed by using the `ProjectExpression` returned by the `ProjectExpression.accessors` function as a springboard via the `partitionKey` and `sortKey` methods.

| AWS|Example|Context
| ---|---|---
| Primary Keys | `Person.id.partitionKey === "1" && Person.year.sortKey === "2020`"  | `GetItem`, `PutItem`, `DeleteItem` 
| Key Condition Expressions | `<query>.whereKey(Person.id.partitionKey === "1" && Person.year.sortKey > 2020)` | `Query`

