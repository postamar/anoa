package com.adgear.anoa.parser;

public class AnoaDependencyException extends AnoaParseException {

  public AnoaDependencyException(Statement statement, String message) {
    super(statement, "Dependency error", message);
  }
}
