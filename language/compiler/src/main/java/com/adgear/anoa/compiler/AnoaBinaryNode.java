package com.adgear.anoa.compiler;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.node.BinaryNode;
import org.codehaus.jackson.node.ValueNode;

import java.io.IOException;
import java.math.BigInteger;
import java.util.regex.Pattern;

/**
 * JsonNode for binary values with various text encodings and utility methods.
 */
final public class AnoaBinaryNode extends ValueNode {

  final private BinaryNode wrapped;

  private AnoaBinaryNode(BinaryNode wrapped) {
    this.wrapped = wrapped;
  }

  public static AnoaBinaryNode valueOf(byte[] bytes) {
    return new AnoaBinaryNode(BinaryNode.valueOf(bytes));
  }

  public static AnoaBinaryNode valueOf(JsonNode node) {
    return new AnoaBinaryNode(BinaryNode.valueOf(
        (node instanceof AnoaBinaryNode)
        ? ((AnoaBinaryNode) node).getBinaryValue()
        : ((node instanceof BinaryNode)
           ? ((BinaryNode) node).getBinaryValue()
           : node.asText().getBytes())));
  }

  @Override
  public JsonToken asToken() {
    return wrapped.asToken();
  }

  @Override
  public void serialize(JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    jgen.writeRawValue(toString());
  }

  @Override
  public String asText() {
    return getTextValue();
  }

  public String toOctalString() {
    StringBuilder sb = new StringBuilder("\"");
    for (byte b : wrapped.getBinaryValue()) {
      sb.append('\\').append(BigInteger.valueOf(((long) b) & 0xff).toString(8));
    }
    return sb.append('\"').toString();
  }

  public Pattern buildThriftBrokenJavaPattern(String fieldName) {
    return Pattern.compile(
        "^(\\s+this\\."
        + fieldName
        + "\\s+=\\s+)"
        + toThriftString()
        + "\\s*;\\s*$");
  }

  public String toThriftString() {
    return toOctalString().replace('\\', '/');
  }

  public String toThriftJava() {
    String bbWrapped = (wrapped.getBinaryValue().length == 0)
                       ? "new byte[0]"
                       : toOctalString() + ".getBytes()";
    return "java.nio.ByteBuffer.wrap(" + bbWrapped + ");";
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) return true;
    if (o == null) return false;
    if (o.getClass() != getClass()) { // final class, can do this
      return false;
    }
    return wrapped.equals(((AnoaBinaryNode) o).wrapped);
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  @Override
  public String toString() {
    return "\"" + getTextValue() + "\"";
  }

  @Override
  public boolean isBinary() {
    return true;
  }

  @Override
  public boolean isTextual() {
    return true;
  }

  @Override
  public String getTextValue() {
    StringBuilder sb = new StringBuilder();
    for (byte b : wrapped.getBinaryValue()) {
      sb.append(String.format("\\u00%02X", b));
    }
    return sb.toString();
  }

  @Override
  public byte[] getBinaryValue() {
    return wrapped.getBinaryValue();
  }
}
