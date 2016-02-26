package com.adgear.anoa.parser;

public class AnoaSyntaxException extends AnoaParseException {

  public AnoaSyntaxException(String name, String message) {
    super(name, 0, "Declaration error", message);
  }

  public AnoaSyntaxException(String origin, int line, String invalidStatement) {
    super(origin, line, "Syntax error", "invalid statement: " + invalidStatement);
  }
}
