package com.adgear.anoa.parser.type;

import com.adgear.anoa.parser.AnoaParseException;
import com.adgear.anoa.parser.state.EnumState;
import com.adgear.anoa.parser.Statement;
import com.adgear.anoa.parser.state.StructState;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.TextNode;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

final public class TypeFactory {

  private TypeFactory() {
  }

  static public FieldType structType(Statement statement,
                                     StructState structDependency)
      throws AnoaParseException {
    if (statement.structFieldDefault.isPresent()) {
      throw new AnoaParseException(statement, "default values not permitted for struct fields");
    }
    return modified(statement, new StructType(structDependency));
  }

  static public FieldType enumType(Statement statement,
                                   EnumState enumDependency)
      throws AnoaParseException {
    final int ordinal;
    List<String> enumSymbols = enumDependency.avroSchema().getEnumSymbols();
    if (statement.structFieldDefault.isPresent()) {
      Optional<String> found = enumSymbols.stream()
          .filter(statement.structFieldDefault.get()::equalsIgnoreCase)
          .findFirst();
      if (!found.isPresent()) {
        throw new AnoaParseException(statement,
                                     "invalid enum default value, must be one of " + enumSymbols);
      }
      ordinal = enumSymbols.indexOf(found.get());
    } else {
      ordinal = 0;
    }
    return modified(statement, new EnumType(enumDependency, ordinal));
  }

  static public FieldType primitiveType(Statement statement) throws AnoaParseException {
    final FieldType wrapped;
    try {
      wrapped = primitiveType(statement.structFieldType.get().toUpperCase(),
                              statement.structFieldDefault);
    } catch (AnoaTypeException e) {
      throw new AnoaParseException(statement, e.getMessage());
    }
    return modified(statement, wrapped);
  }

  static FieldType modified(Statement statement, FieldType value) {
    switch (statement.structFieldModifier.orElse("").toUpperCase()) {
      case "LIST":
        return new WrapperList(value);
      case "MAP":
        return new WrapperMap(value);
      default:
        return new WrapperSingle(value);
    }
  }

  static FieldType primitiveType(String type, Optional<String> value)
      throws AnoaTypeException {
    switch (type) {
      case "BOOLEAN":
        return booleanType(value.orElse("false"));
      case "BYTES":
        return bytesType(value.orElse(""));
      case "FLOAT":
        return floatType(value.orElse("0"));
      case "DOUBLE":
        return doubleType(value.orElse("0"));
      case "INT":
        return intType(value.orElse("0"));
      case "LONG":
        return longType(value.orElse("0"));
      case "STRING":
        return stringType(value.orElse(""));
      default:
        throw new AnoaTypeException("Unknown primitive type '" + type + "'");
    }
  }

  static FieldType booleanType(String text) throws AnoaTypeException {
    final BooleanNode node;
    switch (text) {
      case "true":
        node = BooleanNode.TRUE;
        break;
      case "false":
        node = BooleanNode.FALSE;
        break;
      default:
        throw new AnoaTypeException("default value for boolean must be either 'true' or 'false'");
    }
    return new PrimitiveType(node, !node.asBoolean(), "bool", "bool", Schema.Type.BOOLEAN);
  }

  static FieldType bytesType(String base64) throws AnoaTypeException {
    byte[] value;
    JsonNode node;
    try {
      node = toJson((base64.charAt(0) == '"') ? base64 : ("\"" + base64 + "\""));
      value = node.getBinaryValue();
    } catch (IOException e) {
      node = null;
      value = null;
    }
    if (value == null) {
      throw new AnoaTypeException("default value for bytes must be base-64 encoded string");
    }
    return new PrimitiveType(node, value.length == 0, "bytes", "binary", Schema.Type.BYTES);
  }

  static FieldType stringType(String str) throws AnoaTypeException {
    String text;
    try {
      text = toJson(str).getTextValue();
    } catch (IOException e) {
      throw new AnoaTypeException("default value for string must be JSON string");
    }
    JsonNode node = TextNode.valueOf(text);
    return new PrimitiveType(node, text.isEmpty(), "string", "string", Schema.Type.STRING);
  }

  static FieldType longType(String integer) throws AnoaTypeException {
    JsonNode node = LongNode.valueOf(toLong(integer));
    return new PrimitiveType(node, node.asLong() == 0L, "sint64", "i64", Schema.Type.LONG);
  }

  static FieldType intType(String integer) throws AnoaTypeException {
    JsonNode node = LongNode.valueOf(toLong(integer));
    return new PrimitiveType(node, node.asLong() == 0L, "sint32", "i32", Schema.Type.INT);
  }

  static FieldType doubleType(String number) throws AnoaTypeException {
    JsonNode node = DoubleNode.valueOf(toDouble(number));
    return new PrimitiveType(node, node.asDouble() == 0.0, "double", "double", Schema.Type.DOUBLE);
  }

  static FieldType floatType(String number) throws AnoaTypeException {
    JsonNode node = DoubleNode.valueOf(toDouble(number));
    return new PrimitiveType(node, node.asDouble() == 0.0, "float", "double", Schema.Type.FLOAT);
  }

  static private ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static public JsonNode toJson(String text) throws IOException {
    ArrayNode array = (ArrayNode) OBJECT_MAPPER.readTree("[" + text + "]");
    if (array.size() != 1) {
      throw new IOException();
    }
    return array.get(0);
  }

  static private long toLong(String text) throws AnoaTypeException {
    try {
      return new BigInteger(text).longValue();
    } catch (NumberFormatException e) {
      throw new AnoaTypeException("default value for long or int must be an integer");
    }
  }


  static private double toDouble(String text) throws AnoaTypeException {
    try {
      return new BigDecimal(text).doubleValue();
    } catch (NumberFormatException e) {
      throw new AnoaTypeException("default value for float or double must be a number");
    }
  }

  static class AnoaTypeException extends Exception {

    AnoaTypeException(String s) {
      super(s);
    }
  }
}
