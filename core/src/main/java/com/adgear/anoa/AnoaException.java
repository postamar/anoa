package com.adgear.anoa;

public class AnoaException extends Exception {

  public AnoaException(String message) {
    super(message);
  }

  public AnoaException(String message, Throwable cause) {
    super(message, cause);
  }

  public AnoaException(Throwable cause) {
    super(cause);
  }
}
