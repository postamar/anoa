package com.adgear.anoa;

public class AnoaTypeException extends AnoaException {

  public AnoaTypeException(String message) {
    super(message);
  }

  public AnoaTypeException(String message, Throwable cause) {
    super(message, cause);
  }

  public AnoaTypeException(Throwable cause) {
    super(cause);
  }
}
