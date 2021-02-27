package zio.dynamodb

// Interestingly scan can be run in parallel using segment number and total segments fields
// If running in parallel segment number must be used consistently with the paging token

// I have introduced ScanSegments model as both fields are required for Scan if any one is present
// number is zero based must be >=0 and < totalSegments
// TODO: use this in parallel scan operations
final case class ScanSegments(number: Int, totalSegments: Int)
