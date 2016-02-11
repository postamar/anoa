package com.adgear.data.plugin;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;

public class ExportProtobuf extends ExportBase {

  static private final ExportProtobuf INSTANCE = new ExportProtobuf();

  static public ExportProtobuf get() {
    return INSTANCE;
  }

  @Override
  protected void buildNameSpace(Schema s, StringBuilder sb) {
    sb.append("syntax = \"proto3\";\npackage proto.").append(s.getNamespace()).append(";\n");
  }

  @Override
  protected void buildInclude(Schema s, Map<String, String> includePaths, StringBuilder sb) {
    sb.append("import public \"").append(includePaths.get(s.getFullName())).append("\";\n");
  }

  @Override
  protected void buildEnumHeader(Schema s, StringBuilder sb) {
    sb.append("enum ").append(s.getName()).append(" {\n");
  }

  @Override
  protected void buildEnumFooter(StringBuilder sb) {
    sb.append("}\n");
  }

  @Override
  protected void buildEnumField(String symbol, int pos, StringBuilder sb) {
    sb.append("  ").append(symbol).append(" = ").append(pos).append(";\n");
  }

  @Override
  protected void buildRecordHeader(Schema s, StringBuilder sb) {
    if (s.getDoc() != null) {
      String doc = s.getDoc();
      Matcher matcher = IDLSchemaMojo.docstringPattern.matcher(doc);
      if (matcher.find()) {
        doc = doc.substring(0, matcher.start());
      }
      for (String line : doc.split("\n")) {
        sb.append("// ").append(line).append('\n');
      }
    }
    sb.append("message ").append(s.getName()).append(" {\n");
  }

  @Override
  protected void buildRecordFooter(StringBuilder sb) {
    sb.append("}\n");
  }

  @Override
  protected void buildRecordField(String id,
                                  Schema.Field field,
                                  Map<String, String> names,
                                  int pos,
                                  StringBuilder sb)
      throws IOException {
    final Schema resolvedSchema = resolveSchema(field.schema());
    final boolean isRepeated = (resolvedSchema.getType() == Schema.Type.ARRAY);
    final String type = getFieldType(resolvedSchema);

    sb.append('\n');
    if (field.doc() != null) {
      for (String line : field.doc().split("\n")) {
        sb.append("  // ").append(line).append('\n');
      }
    }
    sb.append(isRepeated ? "  repeated " : "  ")
        .append(type)
        .append(' ')
        .append(field.name())
        .append(" = ")
        .append(Integer.parseInt(id));
    if (isRepeated) {
      switch (resolveSchema(resolvedSchema.getElementType()).getType()) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          sb.append(" [packed=true]");
          break;
      }
    }
    sb.append(";\n");
  }

  protected String getFieldType(Schema s) throws IOException {
    switch (s.getType()) {
      case ARRAY:
        Schema elementSchema = resolveSchema(s.getElementType());
        if (elementSchema.getType() == Schema.Type.ARRAY
            || elementSchema.getType() == Schema.Type.MAP
            || elementSchema.getType() == Schema.Type.NULL
            || elementSchema.getType() == Schema.Type.UNION) {
          throw new IOException("Complex lists not supported in Protobuf.");
        }
        return getFieldType(elementSchema);
      case BOOLEAN:
        return "bool";
      case BYTES:
      case FIXED:
        return "bytes";
      case DOUBLE:
        return "double";
      case FLOAT:
        return "float";
      case INT:
        return "int32";
      case LONG:
        return "int64";
      case MAP:
        Schema valueSchema = resolveSchema(s.getValueType());
        if (valueSchema.getType() == Schema.Type.ARRAY
            || valueSchema.getType() == Schema.Type.MAP
            || valueSchema.getType() == Schema.Type.NULL
            || valueSchema.getType() == Schema.Type.UNION) {
          throw new IOException("Complex maps not supported in Protobuf.");
        }
        return "map<string, " + getFieldType(valueSchema) + ">";
      case ENUM:
      case RECORD:
        return s.getName();
      case STRING:
        return "string";
      case UNION:
        throw new IOException("Avro type union not supported in Protobuf "
                              + "other than for record fields: union {null, <optional type>}...");
    }
    throw new IOException("Avro type " + s.getType().getName() + " not supported in Protobuf.");
  }
}
