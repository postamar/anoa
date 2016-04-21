package com.adgear.anoa.compiler;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;

/**
 * Custom Anoa interface java source code generator.
 */
public class InterfaceJavaGenerator extends JavaGeneratorBase {

  final public boolean withAvro;
  final public boolean withProtobuf;
  final public boolean withThrift;

  public InterfaceJavaGenerator(Protocol protocol,
                                boolean withAvro,
                                boolean withProtobuf,
                                boolean withThrift) {
    super(protocol);
    setTemplateDir("/com/adgear/anoa/interface/");
    this.withAvro = withAvro;
    this.withProtobuf = withProtobuf;
    this.withThrift = withThrift;
  }

  public boolean isWithAvro() {
    return withAvro;
  }

  public String avroClassName(Schema schema) {
    return anoaInterfaceFullName(schema) + "Avro";
  }

  public boolean isWithProtobuf() {
    return withProtobuf;
  }

  public String protobufClassName(Schema schema) {
    String ns = schema.getNamespace();
    ns += "." + ns.substring(ns.lastIndexOf('.') + 1) + "_protobuf";
    String protobufClassName = AnoaParserBase.capitalizeQualified(ns);
    return mangle(AnoaParserBase.capitalizeQualified(protobufClassName + "." + schema.getName()));
  }

  public String protobufProtocolClassName() {
    return protocolFullName + "Protobuf";
  }

  public boolean isWithThrift() {
    return withThrift;
  }

  public String thriftClassName(Schema schema) {
    return anoaInterfaceFullName(schema) + "Thrift";
  }

  static final public String VALUE = "value";

  static public String generateIsMethod(Schema schema, String token) {
    StringBuilder sb = new StringBuilder("is");
    for (String blob : token.split("_")) {
      sb.append(blob.charAt(0)).append(blob.substring(1).toLowerCase());
    }
    return sb.toString();
  }

  static public String generateCmpMethod(Schema schema, Schema.Field field) {
    return "compare" + generateGetMethod(schema, field).substring(3);
  }

  static public final String CMP_A = "fa";
  static public final String CMP_B = "fb";

  public String generateCmpMethodBody(Schema.Field field) {
    String a = "a_it" + field.pos();
    String b = "b_it" + field.pos();
    switch (field.schema().getType()) {
      case ARRAY:
        Schema e = field.schema().getElementType();
        return "int _cmp = 0; "
               + "java.util.Iterator<" + anoaValueType(e) + "> " + a
               + " = " + CMP_A + ".iterator(); "
               + "java.util.Iterator<" + anoaValueType(e) + "> " + b
               + " = " + CMP_B+ ".iterator(); "
               + "while (" + a + ".hasNext() && " + b + ".hasNext()) { if (0 != (_cmp = "
               + compareSimpleField(e, a + ".next()", b + ".next()") + ")) return _cmp; } "
               + "if (" + a + ".hasNext()) return 1; if (" + b + ".hasNext()) return -1; return 0;";
      case MAP:
        String ae = "_ae" + field.pos();
        String be = "_be" + field.pos();
        Schema v = field.schema().getValueType();
        String et = "java.util.Map.Entry<java.lang.String," + anoaValueType(v) + ">";
        return "int _cmp = 0; "
               + "java.util.Iterator<" + et + "> " + a + " = " + CMP_A + ".entrySet().iterator(); "
               + "java.util.Iterator<" + et + "> " + b + " = " + CMP_B + ".entrySet().iterator(); "
               + "while (" + a + ".hasNext() && " + b + ".hasNext()) { "
               + et + " " + ae + " = " + a + ".next(); " + et + " " + be + " = " + b + ".next(); "
               + "if (0 != (_cmp = " + ae + ".getKey().compareTo(" + be + ".getKey()))) "
               + "return _cmp; "
               + "if (0 != (_cmp = " + compareSimpleField(v, ae + ".getValue()", be + ".getValue()")
               + ")) return _cmp; } "
               + "if (" + a + ".hasNext()) return 1; if (" + b + ".hasNext()) return -1; return 0;";
      default:
        return "return " +  compareSimpleField(field.schema(), CMP_A, CMP_B) + ";";
    }
  }

  private String compareSimpleField(Schema s, String a, String b) {
    switch (s.getType()) {
      case BOOLEAN:
        return "((" + a + ") ? 1 : 0) - ((" + b + ") ? 1 : 0)";
      case BYTES:
        return "java.nio.ByteBuffer.wrap(" + a + ".get())"
               + ".compareTo(java.nio.ByteBuffer.wrap(" + b + ".get()))";
      case ENUM:
        return a + ".getOrdinal() - " + b + ".getOrdinal()";
      case INT:
        return "java.lang.Integer.compare"
               + (GeneratorBase.isUnsigned(s) ? "Unsigned(" : "(")
               + a + ", " + b + ")";
      case LONG:
        return "java.lang.Long.compare"
               + (GeneratorBase.isUnsigned(s) ? "Unsigned(" : "(")
               + a + ", " + b + ")";
      case FLOAT:
        return "java.lang.Float.compare(" + a + ", " + b + ")";
      case DOUBLE:
        return "java.lang.Double.compare(" + a + ", " + b + ")";
      default:
        return a + ".compareTo(" + b + ")";
    }
  }

  public boolean hasAvroExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
      case STRING:
      case ENUM:
      case RECORD:
      case ARRAY:
      case MAP:
        return true;
      default:
        return false;
    }
  }

  public String avroExportValue(Schema.Field field) {
    String value = "get()." + mangle(field.name());
    final String unmod = "java.util.Collections.unmodifiable";
    switch (field.schema().getType()) {
      case BYTES:
        return "java.util.Optional.of(" + value + ")"
               + ".map(bb -> {" + BYTES_SUPPLIER + "}).get()";
      case STRING:
        return "java.util.Optional.of(" + value + ").map(java.lang.Object::toString).orElse(\"\")";
      case ENUM:
      case RECORD:
        return mangle(field.schema().getFullName()) + ".avro(" + value + ")";
      case ARRAY:
        if (isSet(field.schema())) {
          return unmod + "SortedSet(" + value + ".stream()"
                 + avroExportArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection("
                 + "() -> new java.util.TreeSet<" + anoaValueType(field.schema().getElementType())
                 + ">())))";
        } else {
          switch (field.schema().getElementType().getType()) {
            case STRING:
            case BYTES:
            case ENUM:
            case RECORD:
              return unmod + "List(" + value + ".stream()"
                     + avroExportArrayMapper(field.schema().getElementType())
                     + ".collect(java.util.stream.Collectors.toCollection("
                     + "() -> new java.util.ArrayList<"
                     + anoaValueType(field.schema().getElementType())
                     + ">(" + value + ".size()))))";
            default:
              return unmod + "List(" + value + ")";
          }
        }
      case MAP:
        return "java.util.Collections.unmodifiableSortedMap(" + value + ".entrySet().stream()"
               + ".collect(java.util.stream.Collectors.toMap("
               + "e -> e.getKey().toString(), "
               + avroExportMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.TreeMap<java.lang.String,"
               + anoaValueType(field.schema().getValueType()) + ">())))";
      default:
        return value;
    }
  }

  private String avroExportArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(bb -> {" + BYTES_SUPPLIER + "})";
      case STRING:
        return ".map(java.lang.Object::toString)";
      case ENUM:
      case RECORD:
        return ".map(" + mangle(s.getFullName()) + "::avro)";
      default:
        return "";
    }
  }

  private String avroExportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return  "e -> { java.nio.ByteBuffer bb = e.getValue(); " + BYTES_SUPPLIER + " }";
      case STRING:
        return "e -> e.getValue().toString()";
      case ENUM:
      case RECORD:
        return "e -> " + mangle(s.getFullName()) + ".avro(e.getValue())";
      default:
        return "e -> e.getValue()";
    }
  }

  public String avroImportValue(Schema schema, Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
        return "java.nio.ByteBuffer.wrap(" + VALUE + ".get())";
      case STRING:
        return "new org.apache.avro.util.Utf8(" + VALUE + ")";
      case ENUM:
      case RECORD:
        return anoaInterfaceFullName(field.schema()) + ".avro(" + VALUE + ").get()";
      case ARRAY:
        return VALUE + ".stream()" + avroImportArrayMapper(field.schema().getElementType())
               + ".collect(java.util.stream.Collectors.toCollection(() -> "
               + "new " + avroType(field.schema()) + "(" + VALUE + ".size(), "
               + avroClassName(schema) + ".SCHEMA$.getFields().get("+ field.pos() +").schema())))";
      case MAP:
        return VALUE + ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
               + "e -> new org.apache.avro.util.Utf8(e.getKey()), "
               + avroImportMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new " + avroType(field.schema()) + "(" + VALUE + ".size())))";
      default:
        return VALUE;
    }
  }

  private String avroImportArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(java.util.function.Supplier::get).map(java.nio.ByteBuffer::wrap)";
      case STRING:
        return ".map(org.apache.avro.util.Utf8::new)";
      case ENUM:
      case RECORD:
        return ".map(" + anoaInterfaceFullName(s) + "::avro)"
               + ".map(java.util.function.Supplier::get)";
    }
    return "";
  }

  private String avroImportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "e -> java.nio.ByteBuffer.wrap(e.getValue().get())";
      case STRING:
        return "e -> new org.apache.avro.util.Utf8(e.getValue())";
      case ENUM:
      case RECORD:
        return "e -> " + anoaInterfaceFullName(s) + ".avro(e.getValue()).get()";
    }
    return "e -> e.getValue()";
  }


  public boolean hasProtobufExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case LONG:
        return (GeneratorBase.getPrecision(field.schema()) == 32);
      case BYTES:
      case ENUM:
      case RECORD:
        return true;
      case ARRAY:
        if (isSet(field.schema())) {
          return true;
        }
        switch (field.schema().getElementType().getType()) {
          case LONG:
            return (GeneratorBase.getPrecision(field.schema().getElementType()) == 32);
          case BYTES:
          case ENUM:
          case RECORD:
            return true;
        }
        return false;
      case MAP:
        return true;
    }
    return false;
  }

  public String protobufExportValue(Schema schema, Schema.Field field) {
    String base = "get()." + generateGetMethod(schema, field).replace("$", "");
    String value = base + ((field.schema().getType() == Schema.Type.ARRAY) ? "List()" : "()");
    if (!hasProtobufExportField(field)) {
      if (field.schema().getType() == Schema.Type.ARRAY
          && field.schema().getElementType().getType() == Schema.Type.STRING) {
        value = "(java.util.List<java.lang.String>)(java.util.List<?>) " + value;
      }
      return value;
    }
    switch (field.schema().getType()) {
      case ARRAY:
        if (isSet(field.schema())) {
          return "java.util.Collections.unmodifiableSortedSet(" + value + ".stream()"
                 + protobufExportArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection("
                 + "() -> new java.util.TreeSet<"
                 + anoaValueType(field.schema().getElementType()) + ">())))";
        } else {
          return "java.util.Collections.unmodifiableList(" + value + ".stream()"
                 + protobufExportArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection("
                 + "() -> new java.util.ArrayList<"
                 + anoaValueType(field.schema().getElementType())
                 + ">(" + base + "Count()))))";
        }
      case MAP:
        return "java.util.Collections.unmodifiableSortedMap(" + value + ".entrySet().stream()"
               + ".collect(java.util.stream.Collectors.toMap("
               + "e -> e.getKey(), "
               + protobufExportMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.TreeMap<java.lang.String,"
               + anoaValueType(field.schema().getValueType()) + ">())))";
      case LONG:
        return "((long) " + value + ") & 0xFFFFFFFFL";
      case BYTES:
        return "() -> " + value + ".toByteArray()";
      default:
        return mangle(field.schema().getFullName()) + ".protobuf(" + value + ")";
    }
  }

  private String protobufExportArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(bs -> new java.util.function.Supplier<byte[]>() { "
                  + "public byte[] get() { return bs.toByteArray();} })";
      case ENUM:
      case RECORD:
        return ".map(" + mangle(s.getFullName()) + "::protobuf)";
      case LONG:
        if (GeneratorBase.getPrecision(s) == 32) {
          return ".map(i -> i.longValue() & 0xFFFFFFFFL)";
        }
      default:
        return "";
    }
  }

  private String protobufExportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "e -> { com.google.protobuf.ByteString bs = e.getValue(); "
               + "return new java.util.function.Supplier<byte[]>() { "
               + "public byte[] get() { return bs.toByteArray();} }; }";
      case ENUM:
      case RECORD:
        return "e -> " + mangle(s.getFullName()) + ".protobuf(e.getValue())";
      case LONG:
        if (GeneratorBase.getPrecision(s) == 32) {
          return "e -> e.getValue().longValue() & 0xFFFFFFFFL";
        }
      default:
        return "e -> e.getValue()";
    }
  }

  public String protobufDefaultTest(Schema schema, Schema.Field field) {
    return defaultTest(schema, field, VALUE, "Protobuf._DEFAULT");
  }

  public String protobufImportField(Schema schema, Schema.Field field) {
    String setter = generateSetMethod(schema, field).replace("$", "");
    String value = VALUE;
    switch (field.schema().getType()) {
      case LONG:
        if (GeneratorBase.getPrecision(field.schema()) == 32) {
          value = "(int) " + value;
        }
        break;
      case BYTES:
        value = "com.google.protobuf.ByteString.copyFrom(" + value + ".get())";
        break;
      case ENUM:
      case RECORD:
        value = mangle(field.schema().getFullName()) + ".protobuf(" + value + ").get()";
        break;
      case ARRAY:
        setter = "addAll" + setter.substring(3);
        String map = protobufImportArrayMapper(field.schema().getElementType());
        if (!map.isEmpty()) {
          value += ".stream()" + map + ".collect(java.util.stream.Collectors.toCollection("
                   + "() -> new java.util.ArrayList(" + value + ".size())))";
        }
        break;
      case MAP:
        setter = "putAll" + setter.substring(3);
        String fn = protobufImportMapValueFunction(field.schema().getValueType());
        value += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
                 + "e -> e.getKey(), " + fn + ", (u,v) -> { "
                 + "throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
                 + "() -> new java.util.HashMap(" + value + ".size())))";
        break;
    }
    return setter + "(" + value + ")";
  }

  private String protobufImportArrayMapper(Schema s) {
    switch (s.getType()) {
      case LONG:
        if (GeneratorBase.getPrecision(s) == 32) {
          return ".map(java.lang.Number::intValue)";
        }
        break;
      case BYTES:
        return ".map(java.util.function.Supplier::get)"
               + ".map(com.google.protobuf.ByteString::copyFrom)";
      case ENUM:
      case RECORD:
        return ".map(" + mangle(s.getFullName()) + "::protobuf)"
               + ".map(java.util.function.Supplier::get)";
    }
    return "";
  }

  private String protobufImportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case LONG:
        if (GeneratorBase.getPrecision(s.getValueType()) == 32) {
          return "e -> e.getValue().intValue()";
        }
        break;
      case BYTES:
        return "e -> com.google.protobuf.ByteString.copyFrom(e.getValue().get())";
      case ENUM:
      case RECORD:
        return "e -> " + mangle(s.getFullName()) + ".protobuf(e.getValue()).get()";
    }
    return "e -> e.getValue()";
  }

  public boolean hasThriftExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
      case STRING:
      case ENUM:
      case RECORD:
      case ARRAY:
      case MAP:
        return true;
    }
    return false;
  }

  public String thriftExportValue(Schema.Field field) {
    String value = "wrapped." + (field.schema().getType() == Schema.Type.BOOLEAN ? "is" : "get")
                   + Character.toUpperCase(field.name().charAt(0))
                   + field.name().substring(1) + "()";
    if (!hasThriftExportField(field)) {
      return ((field.schema().getType() == Schema.Type.FLOAT) ? "(float) " : "") + value;
    }
    value = "java.util.Optional.ofNullable(" + value + ")";
    switch (field.schema().getType()) {
      case BYTES:
        return value + ".orElseGet(() -> new byte[0])::clone";
      case STRING:
        return value + ".orElse(\"\")";
      case ENUM:
        return mangle(field.schema().getFullName()) + ".thrift(" + value
               + ".map(org.apache.thrift.TEnum::getValue).orElse(0))";
      case RECORD:
        return mangle(field.schema().getFullName()) + ".thrift(" + value + ".orElseGet("
               + thriftClassName(field.schema()) + "::new))";
      case ARRAY:
        return value + ".orElseGet(java.util.Collections::empty"
               + (isSet(field.schema()) ? "Set" : "List") + ").stream()"
               + thriftExportArrayMapper(field.schema().getElementType()) + ".collect("
               + "java.util.stream.Collectors.toCollection("
               + "() -> new java.util." + (isSet(field.schema()) ? "TreeSet" : "ArrayList")
               + "<" + anoaValueType(field.schema().getElementType()) + ">()))";
      case MAP:
        return value + ".orElseGet(java.util.Collections::emptyMap).entrySet().stream().collect("
               + "java.util.stream.Collectors.toMap("
               + "e -> e.getKey(), "
               + thriftExportMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.TreeMap<java.lang.String,"
               + anoaValueType(field.schema().getValueType()) + ">()))";
      default:
        return value;
    }
  }

  private String thriftExportArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return ".map(bb -> {" + BYTES_SUPPLIER + "})";
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return ".map(java.lang.Number::" + s.getType().toString().toLowerCase() + "Value)";
      case ENUM:
      case RECORD:
        return  ".map(" + mangle(s.getFullName()) + "::thrift)";
    }
    return "";
  }

  private String thriftExportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "e -> { java.nio.ByteBuffer bb = e.getValue(); " + BYTES_SUPPLIER + " }";
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return "e -> e.getValue()." + s.getType().toString().toLowerCase() + "Value()";
      case ENUM:
      case RECORD:
        return "e -> " + mangle(s.getFullName()) + ".thrift(e.getValue())";
    }
    return "e -> e.getValue()";
  }

  public String thriftDefaultTest(Schema schema, Schema.Field field) {
    return defaultTest(schema, field, VALUE, "Thrift._DEFAULT");
  }

  public String thriftImportField(Schema schema, Schema.Field field) {
    String setter = "set" + Character.toUpperCase(field.name().charAt(0))
                    + field.name().substring(1);
    String value = VALUE;
    switch (field.schema().getType()) {
      case FLOAT:
        value = "(double) " + value;
        break;
      case INT:
        switch (ThriftGenerator.getThriftPrecision(field.schema())) {
          case 8:  value = "(byte) "  + value; break;
          case 16: value = "(short) " + value; break;
          default: value = "(int) "   + value;
        }
        break;
      case LONG:
        value = "(long) " + value;
        break;
      case BYTES:
        value = "java.nio.ByteBuffer.wrap(" + value + ".get())";
        break;
      case ENUM:
      case RECORD:
        value = mangle(field.schema().getFullName()) + ".thrift(" + value + ").get()";
        break;
      case ARRAY:
        value += ".stream()" + thriftImportArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection(() ->"
                 + " new java.util." + (isSet(field.schema()) ? "HashSet" : "ArrayList")
                 + "<>(" + value + ".size())))";
        break;
      case MAP:
        value += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
                 + "e -> e.getKey(), "
                 + thriftImportMapValueFunction(field.schema().getValueType()) + ", (u,v) -> {"
                 + "throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
                 + "() -> new java.util.HashMap<>(" + value + ".size())))";
    }
    return setter + "(" + value + ")";
  }

  private String thriftImportArrayMapper(Schema s) {
    switch (s.getType()) {
      case FLOAT:
        return ".map(java.lang.Float::doubleValue)";
      case LONG:
      case INT:
        switch (ThriftGenerator.getThriftPrecision(s)) {
          case 64:
            return ".map(java.lang.Number::longValue)";
          case 32:
            return ".map(java.lang.Number::intValue)";
          case 16:
            return ".map(java.lang.Number::shortValue)";
          case 8:
            return ".map(java.lang.Number::byteValue)";
        }
        break;
      case BYTES:
        return ".map(java.util.function.Supplier::get).map(java.nio.ByteBuffer::wrap)";
      case ENUM:
      case RECORD:
        return  ".map(" + mangle(s.getFullName()) + "::thrift)"
                + ".map(java.util.function.Supplier::get)";
    }
    return "";
  }

  private String thriftImportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case FLOAT:
        return "e -> e.getValue().doubleValue()";
      case LONG:
      case INT:
        switch (ThriftGenerator.getThriftPrecision(s)) {
          case 64:
            return "e -> e.getValue().longValue()";
          case 32:
            return "e -> e.getValue().intValue()";
          case 16:
            return "e -> e.getValue().shortValue()";
          case 8:
            return "e -> e.getValue().byteValue()";
        }
        break;
      case BYTES:
        return "e -> java.nio.ByteBuffer.wrap(e.getValue().get())";
      case ENUM:
      case RECORD:
        return "e -> " + mangle(s.getFullName()) + ".thrift(e.getValue()).get()";
    }
    return "e -> e.getValue()";
  }
}
