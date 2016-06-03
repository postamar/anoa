package com.adgear.anoa.compiler.javagen;

import com.adgear.anoa.compiler.AnoaBinaryNode;
import com.adgear.anoa.compiler.CompilationUnit;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.List;

/**
 * Custom Anoa interface java source code generator.
 */
final public class InterfaceJavaGenerator
    extends AbstractJavaGenerator {

  final public boolean withAvro;
  final public boolean withProtobuf;
  final public boolean withThrift;

  public NativeImplJavaGenerator nativeImpl;
  public AvroImplJavaGenerator avro;
  public ProtobufImplJavaGenerator protobuf;
  public ThriftImplJavaGenerator thrift;

  public AbstractImplJavaGenerator impl;

  private Schema schema;

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

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }

  public String className() {
    return impl.className();
  }

  public String anoaWrapperName() {
    return impl.wrapperName();
  }

  public String anoaWrapperFullName() {
    return anoaInterfaceFullName() + "." + impl.wrapperName();
  }

  public boolean hasExportField(Schema.Field field) {
    return impl.hasExportField(field);
  }

  public String exportValue(Schema.Field field) {
    return impl.exportValue(field);
  }

  public String importValue(Schema.Field field) {
    return impl.importValue(field);
  }

  public boolean isWithNative() {
    impl = nativeImpl = new NativeImplJavaGenerator(this);
    return true;
  }

  public boolean isWithAvro() {
    if (withAvro) {
      impl = avro = new AvroImplJavaGenerator(this);
    }
    return withAvro;
  }

  public boolean isWithProtobuf() {
    if (withProtobuf) {
      impl = protobuf = new ProtobufImplJavaGenerator(this);
    }
    return withProtobuf;
  }
  public boolean isWithThrift() {
    if (withThrift) {
      impl = thrift = new ThriftImplJavaGenerator(this);
    }
    return withThrift;
  }

  public String getMethod(Schema.Field field) {
    return generateGetMethod(getSchema(), field);
  }

  public String setMethod(Schema.Field field) {
    return generateSetMethod(getSchema(), field);
  }

  public String clearMethod(Schema.Field field) {
    return generateClearMethod(getSchema(), field);
  }

  public String IsDefaultMethod(Schema.Field field) {
    return generateIsDefaultMethod(getSchema(), field);
  }

  public String defaultTest(Schema.Field field) {
    return defaultMethodTest(schema,
                             field,
                             generateGetMethod(schema, field) + "()",
                             anoaWrapperFullName() + "._DEFAULT");
  }

  public String builderDefaultTest(Schema.Field field) {
    return defaultMethodTest(schema,
                             field,
                             AbstractImplJavaGenerator.VALUE,
                             anoaWrapperFullName() + "._DEFAULT");
  }

  public String exportType(Schema.Field field) {
    return anoaType(field.schema(), true);
  }

  public String importType(Schema.Field field) {
    return anoaType(field.schema(), false);
  }

  public String exportFieldType(Schema.Field field) {
    switch (field.schema().getType()) {
      case ARRAY:
      case MAP:
        return anoaType(field.schema(), true);
      default:
        return anoaValueType(field.schema());
    }
  }

  public String anoaInterfaceName() {
    return anoaInterfaceName(getSchema());
  }

  public String anoaInterfaceFullName() {
    return anoaInterfaceFullName(getSchema());
  }

  public List<Schema.Field> fields() {
    return fields(getSchema());
  }

  public String version() {
    return version(getSchema());
  }

  public String builderClassName() {
    return className() + "Builder";
  }

  public String builderClearMethod(Schema.Field field) {
    return impl.builderClearMethod(field);
  }

  public String isMethod(String token) {
    StringBuilder sb = new StringBuilder("is");
    for (String blob : token.split("_")) {
      sb.append(blob.charAt(0)).append(blob.substring(1).toLowerCase());
    }
    return sb.toString();
  }

  public String isDefaultMethod(Schema.Field field) {
    return generateIsDefaultMethod(schema, field);
  }

  static public String generateIsDefaultMethod(Schema schema, Schema.Field field) {
    return "isDefault" + generateGetMethod(schema, field).substring(3);
  }

  static public String exportFieldName(Schema.Field field) {
    return "_" + mangle(field.name());
  }

  static public String isDefaultFieldName(Schema.Field field) {
    return "is_default$" + mangle(field.name());
  }

  static public String anoaInterfaceName(Schema schema) {
    return mangle(schema.getName());
  }

  static public String anoaInterfaceFullName(Schema schema) {
    return mangle(schema.getFullName());
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
      case ARRAY:   return "java.util."
                           + (CompilationUnit.isSet(s) ? (export ? "SortedSet<" : "Set<") : "List<")
                           + anoaValueType(s.getElementType()) + ">";
      case MAP:     return "java.util." + (export ? "Sorted" : "") + "Map<java.lang.String,"
                           + anoaValueType(s.getValueType()) + ">";
      default: return anoaValueType(s);
    }
  }

  static protected String defaultMethodTest(Schema schema,
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

  public String cmpMethod(Schema.Field field) {
    return generateCmpMethod(schema, field);

  }

  static public String generateCmpMethod(Schema schema, Schema.Field field) {
    return "compare" + generateGetMethod(schema, field).substring(3);
  }

  static public final String CMP_A = "fa";
  static public final String CMP_B = "fb";

  public String cmpMethodBody(Schema.Field field) {
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
               + (CompilationUnit.isUnsigned(s) ? "Unsigned(" : "(")
               + a + ", " + b + ")";
      case LONG:
        return "java.lang.Long.compare"
               + (CompilationUnit.isUnsigned(s) ? "Unsigned(" : "(")
               + a + ", " + b + ")";
      case FLOAT:
        return "java.lang.Float.compare(" + a + ", " + b + ")";
      case DOUBLE:
        return "java.lang.Double.compare(" + a + ", " + b + ")";
      default:
        return a + ".compareTo(" + b + ")";
    }
  }

  public String protobufProtocolClassName() {
    return protocolFullName + "Protobuf";
  }

  public String nativeFieldType(Schema.Field field) {
    return nativeImpl.nativeFieldType(field);
  }

  public String nativeFieldName(Schema.Field field) {
    return nativeImpl.nativeFieldName(field);
  }

  public String nativeDefaultValue(Schema.Field field) {
    return nativeImpl.nativeDefaultValue(field);
  }

  public String nativeToString(Schema.Field field) {
    return nativeImpl.nativeToString(field);
  }
}
