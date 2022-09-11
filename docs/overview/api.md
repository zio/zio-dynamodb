---
id: overview_api
title: "API"
---

## High Level API

### Codec Generation with ZIO-Schema

ZIO-DynamoDB leverages ZIO-Schema heavily to provide an API that type safe and simple to use. Developers can generate a codec for their case classes and immediately begin working with DynamoDB.

```scala
implicit val studentSchema: Schema[Student] = DeriveSchema.gen[Student]

val firstStudent = Student(
  email   = "student@gmail.com",
  subject = "arts"
)

val writtenStudent: ZIO[DynamoDBExecutor, Throwable, Unit] = put[Student]("studentsTable", firstStudent).execute
```

Reified Optics


## Low Level API

The lower level API is available in case developers need access to a DynamoDB feature not provided by the higher level API.

### AttributeValue
`AttributeValue` & `AttrMap`
