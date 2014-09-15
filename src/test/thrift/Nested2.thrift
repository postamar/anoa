namespace * com.adgear.generated.thrift

include "BrowserType.thrift"
include "Simple.thrift"

/** Test nested record - thrift serializable */
struct Nested2 {
  /** [1] */
  1: optional BrowserType.BrowserType type = BrowserType.IE,
  /** [2] */
  2: optional map<string, list<Simple.Simple>> ugly,
  /** [3] */
  3: required list<double> numbers
}
