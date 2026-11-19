---
id: resources
title: "Resources"
---

Talks about ZIO DynamoDB by its authors:

- [Introducing ZIO DynamoDB by Avinder Bahra & Adam Johnson](https://www.youtube.com/watch?v=f68-69eA8Vc&t=33s) - DynamoDB powers many cloud-scale applications, with its robust horizontal scalability and uptime. Yet, interacting with the Java SDK is error-prone and tedious. In this presentation, Avinder Bahra presents ZIO DynamoDB, a new library by Avi and Adam Johnson designed to make interacting with DynamoDB easy, type-safe, testable, and productive.
- [Introducing The ZIO DynamoDB Type-Safe API by Avinder Bahra](https://www.youtube.com/watch?v=Qte4WUfHQ3g&t=10s) - Adam Johnson and Avinder released ZIO DynamoDB, a new Scala library that significantly reduces boilerplate when compared to working directly with AWS client libraries. However, there was still work to be done to improve type safety. In this talk, Avinder introduces a new type-safe API that can prevent many errors at compile time while remaining user-friendly.

Runnable examples live in the [examples sbt module](https://github.com/zio/zio-dynamodb/tree/series/2.x/examples/src/main/scala/zio/dynamodb/examples), and the [integration tests](https://github.com/zio/zio-dynamodb/tree/series/2.x/dynamodb/src/it/scala/zio/dynamodb) exercise every query type against DynamoDB Local.
