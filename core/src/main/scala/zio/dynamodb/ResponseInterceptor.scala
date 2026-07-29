package zio.dynamodb

/**
 * Callback invoked after each AWS DynamoDB operation completes. The returned
 *  `F[Unit]` is sequenced into the interpreter before the caller receives the
 *  result, so side effects (logging, metrics, accumulation) are guaranteed to run.
 *
 *  Implementations must be thread-safe when shared across concurrent requests.
 *  Use [[ZioResponseInterceptor.accumulating]], [[CEResponseInterceptor.accumulating]],
 *  or [[FutureResponseInterceptor.accumulating]] for a ready-made accumulator.
 */
trait ResponseInterceptor[F[_]] {

  /**
   * Called after each DynamoDB data operation completes successfully.
   *  The effect is sequenced before the result is returned to the caller.
   */
  def onResponse(meta: DynamoDBResponseMetadata): F[Unit]
}
