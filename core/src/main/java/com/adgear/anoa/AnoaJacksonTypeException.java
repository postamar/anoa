package com.adgear.anoa;

/**
 * Exception raised when json -> avro / thrift deserialization fails.
 */
public class AnoaJacksonTypeException extends RuntimeException {

  public AnoaJacksonTypeException(String message) {
    super(message);
  }

  public AnoaJacksonTypeException(String message, Throwable cause) {
    super(message, cause);
  }

  public AnoaJacksonTypeException(Throwable cause) {
    super(cause);
  }
}
