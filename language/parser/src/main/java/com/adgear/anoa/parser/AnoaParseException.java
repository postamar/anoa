package com.adgear.anoa.parser;

public class AnoaParseException extends IllegalStateException {

  public AnoaParseException(Statement statement, String message) {
    this(statement, null, message);
  }

  protected AnoaParseException(Statement statement, String errorType, String message) {
    this(statement.origin, statement.line, errorType, message);
  }

  protected AnoaParseException(String origin, int line, String errorType, String message) {
    super(buildMessage(origin, line, errorType, message));
  }

  static private String buildMessage(String origin, int line, String errorType, String message) {
    StringBuilder sb = new StringBuilder();
    sb.append((errorType == null) ? "Error" : errorType);
    if (origin != null) {
      sb.append(" in ").append(origin);
      if (line > 0) {
        sb.append(',').append(line);
      }
    }
    return sb.append(" : ").append(message).toString();
  }
}
