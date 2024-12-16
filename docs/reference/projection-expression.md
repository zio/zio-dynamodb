---
id: projection-expression
title: "ProjectionExpression"
---

The `ProjectionExpression` API is common to both the Type Safe High Level API (with the exception the primary keys API) 
and the Low Level API - however the way one is constructed is different.

**High Level API construction**

```scala
final case class Person(id: String, group: String name: String)
object Person {
  implicit val schema: Schema.CaseClass3[String, String, String, Person] = DeriveSchema.gen[Person]
  
  // id and name are ProjectionExpressions
  val (id, group, name) = ProjectionExpression.accessors[Person]
}
```

**Low Level API construction**

```scala
// id and name are ProjectionExpressions
val id = ProjectionExpression.$("id")
val name = ProjectionExpression.$("name")
```
For more details see [$ and Parse functions](low-level-api/dollar-function) section

**Type Safety**

The other notable difference is that High Level API is totally type safe and many subtle errors are caught at compile time:

```scala
Person.id === 1 // will not compile
Person.id === "1" && Person.name === 2 // will not compile
Person.id === "1" && Employee.name === "2" // will not compile

ProjectionExpression.$("name") === 1 // will compile!
```

### `ProjectionExpression` API Summary

Once we have a `ProjectionExpression` we can use it as a springboard to create further expressions such as 
`ConditionExpression`, `UpdateExpression` and `PrimaryKeyExpression`:

 Category                                                    | Method               | Description
-------------------------------------------------------------|----------------------|---
 Primary Keys (High Level API only)                          | `partitionKey`       | `Person.id.partitionKey === "1"`
 <br/><br/>            | `sortKey`            | `Person.id.partitionKey === "1" && Person.group.sortKey === "2"`
`ConditionExpression` / `FilterExpression` | `===`                | Equality
 <br/><br/>            | `<>`                 | Inequality
 <br/><br/>            | `>`                  | Greater than
 <br/><br/>            | `>=`                 | Greater than or equal
 <br/><br/>            | `<`                  | Less than
 <br/><br/>            | `<=`                 | Less than or equal
 <br/><br/>            | `exists`             | returns true if the attribute exists
 <br/><br/>            | `notExists`          | returns true if the attribute does not exists
 <br/><br/>            | `size`               | returns size of attribute. Applies to all types except Number and Boolean
 <br/><br/>            | `isXXXX`             | There is a function per type eg `isBinary` - returns true if the attribute is of the specified type
 <br/><br/>            | `between(from, to)`  | returns true if the attribute is between the supplied values, inclusive
 <br/><br/>            | `beginsWith`         | Only applies to string attributes
 <br/><br/>            | `inSet`              | returns true if the attribute is in the supplied set
 <br/><br/>            | `in(a, b, c ...)`    | returns true if the attribute matches one of the supplied values
<br/><br/>            | `contains`           | returns true if the attribute contains the supplied value - applies to a String or a Set
`UpdateExpression`| `+`                  | combines update actions eg `Person.name.set(42) + Person.age.set(42)`  
<br/><br/>            | `set`                | Set an attribute `Person.name.set("John")`                                                                         
<br/><br/>            | `setIfNotExists`     | Set attribute if it does not exists `Person.name.setIfNotExists("John")`                                           
<br/><br/>            | `appendList`         | Add supplied list to the end of this list attribute                                                                
<br/><br/>            | `prepend`            | Prepend an element to a list attribute                                                                             
<br/><br/>            | `prependList`        | Prepend a list to list attribute                                                                                   
<br/><br/>            | `deleteFromSet`      | delete all elements that match the supplied set                                                                    
<br/><br/>            | `add(a: To)`         | adds this value as a number attribute if it does not exists, else adds the numeric value to the existing attribute 
<br/><br/>            | `addSet`             | Adds this set as an attribute if it does not exists, else if it exists it adds the elements of the set             
<br/><br/>            | `remove(index: Int)` | remove an element at the specified index                                                                           
<br/><br/>            | `remove`             | Removes this path expression from an item                                                                          

