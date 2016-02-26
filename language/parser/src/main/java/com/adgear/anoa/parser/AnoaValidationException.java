package com.adgear.anoa.parser;


public class AnoaValidationException extends AnoaParseException {

  public AnoaValidationException(String origin, String message) {
    super(origin, -1, "Declaration validation error", message);
  }
}
