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

  public String cast(Schema.Field field) {
    switch (field.schema().getType()) {
      case MAP:
        return "(" + exportType(field.schema()) + ")(java.util.Map<?,?>) ";
      case ARRAY:
        return "(" + exportType(field.schema()) + ")(java.util.List<?>) ";
    }
    return "";
  }

  public boolean hasProtobufExportField(Schema.Field field) {
    switch (field.schema().getType()) {
      case BYTES:
      case ENUM:
      case RECORD:
        return true;
      case ARRAY:
        switch (field.schema().getElementType().getType()) {
          case BYTES:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case ENUM:
          case RECORD:
            return true;
        }
        return false;
      case MAP:
        switch (field.schema().getValueType().getType()) {
          case BYTES:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case ENUM:
          case RECORD:
            return true;
        }
        return false;
    }
    return false;
  }

  public String protobufExportValue(Schema schema, Schema.Field field) {
    String base = "get()." + generateGetMethod(schema, field).replace("$", "");
    String value = base + ((field.schema().getType() == Schema.Type.ARRAY) ? "List()" : "()");
    if (!hasProtobufExportField(field)) {
      switch (field.schema().getType()) {
        case MAP:
          return "(" + exportType(field.schema()) + ")(java.util.Map<?,?>) " + value;
        case ARRAY:
          if (field.schema().getElementType().getType() == Schema.Type.STRING) {
            return "(java.util.List<java.lang.CharSequence>)(java.util.List<?>) " + value;
          }
      }
      return value;
    }
    switch (field.schema().getType()) {
      case ARRAY:
        switch (field.schema().getElementType().getType()) {
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
            value = "((java.util.List<? extends java.lang.Number>) " + value + ")";
        }
        return "java.util.Collections.unmodifiableList(" + value + ".stream().map("
               + protobufExportArrayMapper(field.schema().getElementType())
               + ").collect(java.util.stream.Collectors.toCollection("
               + "() -> new java.util.ArrayList(" + base + "Count()))))";
      case MAP:
        switch (field.schema().getValueType().getType()) {
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
            value = "((java.util.Map<? extends java.lang.CharSequence,? extends java.lang.Number>) "
                    + value + ")";
        }
        return "java.util.Collections.unmodifiableMap(" + value + ".entrySet().stream().collect("
               + "java.util.stream.Collectors.toMap(e -> e.getKey().toString(), "
               + protobufExportMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.HashMap(" + value + ".size()))))";
      case BYTES:
        return "() -> " + value + ".toByteArray()";
      default:
        return mangle(field.schema().getFullName()) + ".Protobuf.from(" + value + ")";
    }
  }

  private String protobufExportArrayMapper(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return "bs -> new java.util.function.Supplier<byte[]>() { "
                  + "public byte[] get() { return bs.toByteArray();} }";
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return "java.lang.Number::" + s.getType().toString().toLowerCase() + "Value";
      default:
        return mangle(s.getFullName()) + ".Protobuf::from";
    }
  }

  private String protobufExportMapValueFunction(Schema s) {
    switch (s.getType()) {
      case BYTES:
        return  "e -> { com.google.protobuf.ByteString bs = e.getValue(); "
                + "return new java.util.function.Supplier<byte[]>() { "
                + "public byte[] get() { return bs.toByteArray();} }; }";
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return "e -> e.getValue()." + s.getType().toString().toLowerCase() + "Value()";
      default:
        return "e -> " + mangle(s.getFullName()) + ".Protobuf.from(e.getValue())";
    }
  }

  public String protobufImportField(Schema schema, Schema.Field field) {
    String setter = generateSetMethod(schema, field).replace("$", "");
    String value = "instance." + generateGetMethod(schema, field) + "()";
    switch (field.schema().getType()) {
      case LONG:
        if (GeneratorBase.getPrecision(field.schema()) == 32) {
          value = "(int) " + value;
        }
        break;
      case BYTES:
        value = "com.google.protobuf.ByteString.copyFrom(" + value + ".get())";
        break;
      case STRING:
        value += ".toString()";
        break;
      case ENUM:
      case RECORD:
        value = mangle(field.schema().getFullName()) + ".Protobuf.from(" + value + ").get()";
        break;
      case ARRAY:
        setter = "addAll" + setter.substring(3);
        String map = "";
        switch (field.schema().getElementType().getType()) {
          case LONG:
            if (GeneratorBase.getPrecision(field.schema().getElementType()) == 32) {
              map = ".map(java.lang.Long::intValue)";
            }
            break;
          case BYTES:
            map = ".map(java.util.function.Supplier::get)"
                  + ".map(com.google.protobuf.ByteString::copyFrom)";
            break;
          case STRING:
            map = ".map(java.lang.Object::toString)";
            break;
          case ENUM:
          case RECORD:
            map = ".map(" + mangle(field.schema().getElementType().getFullName())
                  + ".Protobuf::from).map(java.util.function.Supplier::get)";
        }
        if (!map.isEmpty()) {
          value += ".stream()" + map + ".collect(java.util.stream.Collectors.toCollection("
                   + "() -> new java.util.ArrayList(" + value + ".size())))";
        }
        break;
      case MAP:
        setter = "putAll" + setter.substring(3);
        String fn = "e -> e.getValue()";
        switch (field.schema().getValueType().getType()) {
          case LONG:
            if (GeneratorBase.getPrecision(field.schema().getValueType()) == 32) {
              fn = "e -> e.getValue().intValue()";
            }
            break;
          case BYTES:
            fn = "e -> com.google.protobuf.ByteString.copyFrom(e.getValue().get())";
            break;
          case STRING:
            fn = "e -> e.getValue().toString()";
            break;
          case ENUM:
          case RECORD:
            fn = "e -> " + mangle(field.schema().getValueType().getFullName())
                 + ".Protobuf.from(e.getValue()).get()";
        }
        value += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
                 + "e -> e.getKey().toString(), " + fn + ", (u,v) -> { "
                 + "throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
                 + "() -> new java.util.HashMap(" + value + ".size())))";
        break;
    }
    return setter + "(" + value + ")";
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
        return mangle(field.schema().getFullName()) + ".Thrift.from(" + value
               + ".map(org.apache.thrift.TEnum::getValue).orElse(0))";
      case RECORD:
        return mangle(field.schema().getFullName()) + ".Thrift.from(" + value + ".orElseGet("
               + thriftClassName(field.schema()) + "::new))";
      case ARRAY:
        return value + ".map(l -> l.stream()"
               + thriftExportArrayMapper(field.schema().getElementType()) + ".collect("
               + "java.util.stream.Collectors.toCollection("
               + "() -> new java.util.ArrayList(l.size()))))"
               + ".orElseGet(() -> new java.util.ArrayList())";
      case MAP:

        return value + ".map(m -> m.entrySet().stream().collect("
               + "java.util.stream.Collectors.toMap("
               + "e -> e.getKey().toString(), "
               + thriftExportMapValueFunction(field.schema().getValueType()) + ", "
               + "(u,v) -> { throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
               + "() -> new java.util.HashMap(m.size()))))"
               + ".orElseGet(() -> new java.util.HashMap())";
      default:
        return value;
    }
  }

  static private String BYTES_SUPPLIER =
      "byte[] b = new byte[bb.remaining()]; "
      + "bb.asReadOnlyBuffer().get(b); "
      + "return (java.util.function.Supplier<byte[]>)(b::clone);";

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
        return  ".map(" + mangle(s.getFullName()) + ".Thrift::from)";
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
        return "e -> " + mangle(s.getFullName()) + ".Thrift.from(e.getValue())";
    }
    return "e -> e.getValue()";
  }

  public String thriftImportField(Schema schema, Schema.Field field) {
    String setter = "set" + Character.toUpperCase(field.name().charAt(0))
                    + field.name().substring(1);
    String value = IMPORTED + "." + generateGetMethod(schema, field) + "()";
    switch (field.schema().getType()) {
      case FLOAT:
        value = "(double) " + value;
        break;
      case LONG:
      case INT:
        switch (GeneratorBase.getPrecision(field.schema())) {
          case 64: value = "(long) "  + value; break;
          case 32: value = "(int) "   + value; break;
          case 16: value = "(short) " + value; break;
          case 8:  value = "(byte) "  + value;
        }
        break;
      case BYTES:
        value = "java.nio.ByteBuffer.wrap(" + value + ".get())";
        break;
      case STRING:
        value += ".toString()";
        break;
      case ENUM:
      case RECORD:
        value = mangle(field.schema().getFullName()) + ".Thrift.from(" + value + ").get()";
        break;
      case ARRAY:
        value += ".stream()" + thriftImportArrayMapper(field.schema().getElementType())
                 + ".collect(java.util.stream.Collectors.toCollection(() ->"
                 + " new java.util.ArrayList(" + value + ".size())))";
        break;
      case MAP:
        value += ".entrySet().stream().collect(java.util.stream.Collectors.toMap("
                 + "e -> e.getKey().toString(), "
                 + thriftImportMapValueFunction(field.schema().getValueType()) + ", (u,v) -> {"
                 + "throw new java.lang.IllegalStateException(\"Duplicate key \" + u); }, "
                 + "() -> new java.util.HashMap(" + value + ".size())))";
    }
    return setter + "(" + value + ")";
  }

  private String thriftImportArrayMapper(Schema s) {
    switch (s.getType()) {
      case FLOAT:
        return ".map(java.lang.Float::doubleValue)";
      case LONG:
      case INT:
        switch (GeneratorBase.getPrecision(s)) {
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
      case STRING:
        return ".map(java.lang.Object::toString)";
      case ENUM:
      case RECORD:
        return  ".map(" + mangle(s.getFullName()) + ".Thrift::from)"
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
        switch (GeneratorBase.getPrecision(s)) {
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
      case STRING:
        return "e -> e.getValue().toString()";
      case ENUM:
      case RECORD:
        return "e -> " + mangle(s.getFullName()) + ".Thrift.from(e.getValue()).get()";
    }
    return "e -> e.getValue()";
  }
}
