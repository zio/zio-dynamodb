package zio.dynamodb

object ExecuteSyntax {
  implicit class DynamoDBQueryOps[In, Out](private val query: DynamoDBQuery[In, Out]) extends AnyVal {
    def execute[F[_]](implicit interpreter: Interpreter[F]): F[Out] =
      interpreter.run(query)
  }
}
