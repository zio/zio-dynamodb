package zio.dynamodb

// number is zero based must be >=0 and < totalSegments
final case class ScanSegments(number: Int, totalSegments: Int)
