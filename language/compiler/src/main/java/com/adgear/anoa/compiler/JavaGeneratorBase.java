package com.adgear.anoa.compiler;

import org.apache.avro.JsonProperties;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonNode;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

/**
 * Base class for custom java source code generator.
 */
abstract class JavaGeneratorBase extends SpecificCompiler {

  final protected String protocolFullName;

  public JavaGeneratorBase(Protocol protocol) {
    super(protocol);
    setStringType(GenericData.StringType.Utf8);
    setFieldVisibility(FieldVisibility.PRIVATE);
    setCreateSetters(false);
    setOutputCharacterEncoding("UTF-8");
    this.protocolFullName = Optional.ofNullable(protocol.getNamespace())
                                .map(ns -> ns + ".")
                                .orElse("")
                            + protocol.getName();
    try {
      Field protocolField = SpecificCompiler.class.getDeclaredField("protocol");
      protocolField.setAccessible(true);
      protocolField.set(this, null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String anoaInterfaceName(Schema schema) {
    return mangle(schema.getName());
  }

  public String anoaInterfaceFullName(Schema schema) {
    return mangle(schema.getFullName());
  }

  public List<Schema.Field> fields(Schema schema) {
    return CompilationUnit.modifySchema(schema, "", false).getFields();
  }

  public String version(Schema schema) {
    if (schema.getType() == Schema.Type.ENUM) {
      return Long.toString(schema.getEnumSymbols().size());
    }
    long largest = 0L;
    for (Schema.Field field : schema.getFields()) {
      largest = Math.max(largest, field.getJsonProp(AnoaParserBase.ORDINAL_PROP_KEY).asLong());
    }
    return Long.toString(largest);
  }

  public boolean isDeprecated(JsonProperties schema) {
    return Optional.ofNullable(schema.getJsonProp("deprecated"))
        .map(JsonNode::asBoolean)
        .orElse(false);
  }

  public Schema.Field aliasField(Schema.Field field, String alias) {
    return new Schema.Field(
        alias,
        field.schema(),
        field.doc(),
        field.defaultValue(),
        field.order());
  }

  protected String anoaValueType(Schema s) {
    switch (s.getType()) {
      case STRING:  return "java.lang.String";
      case BYTES:   return "java.util.function.Supplier<byte[]>";
      case INT:     return "java.lang.Integer";
      case LONG:    return "java.lang.Long";
      case FLOAT:   return "java.lang.Float";
      case DOUBLE:  return "java.lang.Double";
      case BOOLEAN: return "java.lang.Boolean";
      default: return anoaInterfaceFullName(s) + "<?>";
    }
  }

  protected String anoaType(Schema s, boolean export) {
    switch (s.getType()) {
      case INT:     return "int";
      case LONG:    return "long";
      case FLOAT:   return "float";
      case DOUBLE:  return "double";
      case BOOLEAN: return "boolean";
      case ARRAY:   return "java.util." + (isSet(s) ? (export ? "SortedSet<" : "Set<") : "List<")
                           + anoaValueType(s.getElementType()) + ">";
      case MAP:     return "java.util." + (export ? "Sorted" : "") + "Map<java.lang.String,"
                           + anoaValueType(s.getValueType()) + ">";
      default: return anoaValueType(s);
    }
  }

  public String exportType(Schema s) {
    return anoaType(s, true);
  }

  public String importType(Schema s) {
    return anoaType(s, false);
  }

  protected boolean isSet(Schema s) {
    return Optional.ofNullable(s.getJsonProp(AnoaParserBase.SET_PROP_KEY))
        .map(JsonNode::asBoolean)
        .orElse(false);
  }

  public String exportFieldType(Schema s) {
    switch (s.getType()) {
      case ARRAY:
      case MAP:
        return anoaType(s, true);
      default:
        return anoaValueType(s);
    }
  }

  public String exportFieldName(Schema.Field field) {
    return "_export_" + mangle(field.name());
  }

  public String isDefaultFieldName(Schema.Field field) {
    return "_is_default_" + mangle(field.name());
  }

  static final public String VALUE = "value";

  public String defaultMethodTest(Schema schema, Schema.Field field) {
    return defaultTest(schema, field, generateGetMethod(schema, field) + "()", "_DEFAULT");
  }

  protected String defaultTest(Schema schema,
                               Schema.Field field,
                               String value,
                               String defaultInstance) {
    JsonNode node = field.defaultValue();
    switch (field.schema().getType()) {
      case ARRAY:
      case MAP:
        return value + ".isEmpty()";
      case ENUM:
        return value + ".getOrdinal() == 0";
      case RECORD:
        return value + ".get().equals("
               + defaultInstance + ".get()." + generateGetMethod(schema, field) + "().get())";
      case BOOLEAN:
        return (node.getBooleanValue() ? "" : "!") + value;
      case INT:
        return Integer.toString(node.getIntValue(), 16) + " == " + value;
      case LONG:
        return Long.toString(node.getLongValue(), 16) + " == " + value;
      case FLOAT:
        return Float.toHexString((float) node.getDoubleValue()) + " == " + value;
      case DOUBLE:
        return Double.toHexString(node.getDoubleValue()) + " == " + value;
      case STRING:
        if (node.getTextValue().isEmpty()) {
          return value + ".length() == 0";
        }
        return node.toString() + ".equals(" + value + ".toString())";
      case BYTES:
        AnoaBinaryNode binaryNode = (AnoaBinaryNode) field.defaultValue();
        if (binaryNode.getBinaryValue().length == 0) {
          return value + ".get().length == 0";
        }
        return "java.util.Arrays.equals(" + value + ".get(), "
               + binaryNode.toOctalString() + ".getBytes())";
    }
    throw new IllegalStateException();
  }

  public String generateIsDefaultMethod(Schema schema, Schema.Field field) {
    return "isDefault" + generateHasMethod(schema, field).substring(3);
  }

  static protected String BYTES_SUPPLIER =
      "byte[] b = new byte[bb.remaining()]; "
      + "bb.asReadOnlyBuffer().get(b); "
      + "return (java.util.function.Supplier<byte[]>)(b::clone);";


  protected String avroInnerType(Schema s) {
    switch (s.getType()) {
      case STRING:  return "org.apache.avro.util.Utf8";
      case BYTES:   return "java.nio.ByteBuffer";
      case INT:     return "java.lang.Integer";
      case LONG:    return "java.lang.Long";
      case FLOAT:   return "java.lang.Float";
      case DOUBLE:  return "java.lang.Double";
      case BOOLEAN: return "java.lang.Boolean";
      default:      return anoaInterfaceFullName(s) + "Avro";
    }
  }

  public String avroType(Schema s) {
    switch (s.getType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return s.getType().toString().toLowerCase();
      case ARRAY:
        return "org.apache.avro.generic.GenericData.Array<"
               + avroInnerType(s.getElementType()) + ">";
      case MAP:
        return "java.util.HashMap<org.apache.avro.util.Utf8,"
               + avroInnerType(s.getValueType()) + ">";
      default:
        return avroInnerType(s);
    }
  }

  protected String avroEntryType(Schema s) {
    if (s.getType() == Schema.Type.MAP) {
      return "java.util.Map.Entry<org.apache.avro.util.Utf8,"
             + avroInnerType(s.getValueType()) + ">";
    }
    return avroInnerType(s.getElementType());
  }

}
