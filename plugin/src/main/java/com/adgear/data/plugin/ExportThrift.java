package com.adgear.data.plugin;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;

public class ExportThrift extends ExportBase {

  static private final ExportThrift INSTANCE = new ExportThrift();

  static protected ObjectMapper objectMapper = new ObjectMapper();

  static public ExportThrift get() {
    return INSTANCE;
  }

  @Override
  protected void buildNameSpace(Schema s, StringBuilder sb) {
    sb.append("namespace * thrift.").append(s.getNamespace()).append('\n');
  }

  @Override
  protected void buildInclude(Schema s, Map<String, String> includePaths, StringBuilder sb) {
    sb.append("include \"").append(includePaths.get(s.getFullName())).append("\"\n");
  }

  @Override
  protected void buildEnumHeader(Schema s, StringBuilder sb) {
    sb.append("enum ").append(s.getName()).append(" {");
  }

  @Override
  protected void buildEnumFooter(StringBuilder sb) {
    sb.append("\n}\n");
  }

  @Override
  protected void buildEnumField(String symbol, int pos, StringBuilder sb) {
    sb.append((pos == 0) ? "\n  " : ",\n  ").append(symbol);
  }

  @Override
  protected void buildRecordHeader(Schema s, StringBuilder sb) {
    if (s.getDoc() != null) {
      String doc = s.getDoc();
      Matcher matcher = IDLSchemaMojo.docstringPattern.matcher(doc);
      if (matcher.find()) {
        doc = doc.substring(0, matcher.start());
      }
      sb.append("/** ").append(doc).append(" */\n");
    }
    sb.append("struct ").append(s.getName()).append(" {");

  }

  @Override
  protected void buildRecordFooter(StringBuilder sb) {
    sb.append("\n}\n");
  }

  @Override
  protected void buildRecordField(String id,
                                  Schema.Field field,
                                  Map<String, String> names,
                                  int pos,
                                  StringBuilder sb)
      throws IOException {
    final Schema resolvedSchema = resolveSchema(field.schema());
    final boolean isOptional = (resolvedSchema != field.schema() || field.defaultValue() != null);
    final String type = getFieldType(resolvedSchema, field.schema().getProp("thrift"), names);
    final String def = (field.defaultValue() == null)
                       ? null
                       : getFieldDefault(resolvedSchema, field.defaultValue());

    sb.append((pos == 0) ? "" : ",").append("\n  ");
    if (field.doc() != null) {
      sb.append("/** ").append(field.doc()).append(" */\n  ");
    }
    sb.append(id)
        .append(isOptional ? ": optional " : ": required ")
        .append(type)
        .append(' ')
        .append(field.name());
    if (def != null) {
      sb.append(" = ").append(def);
    }
  }

  protected String getFieldType(Schema s, String thriftType, Map<String, String> names) throws IOException {
    switch (s.getType()) {
      case ARRAY:
        return ("set".equals(thriftType) ? "set" : "list") +
               "<" + getFieldType(s.getElementType(), thriftType, names) + ">";
      case BOOLEAN:
        return "bool";
      case BYTES:
      case FIXED:
        return "binary";
      case DOUBLE:
      case FLOAT:
        return "double";
      case INT:
        if ("byte".equals(thriftType)) {
          return "byte";
        } else if ("short".equals(thriftType)) {
          return "i16";
        } else {
          return "i32";
        }
      case LONG:
        return "i64";
      case MAP:
        return "map<string, " + getFieldType(s.getValueType(), thriftType, names) + ">";
      case ENUM:
      case RECORD:
        return names.get(s.getFullName()) + "." + s.getName();
      case STRING:
        return "string";
      case UNION:
        throw new IOException("Avro type union not supported in Thrift "
                              + "other than for record fields: union {null, <optional type>}...");
    }
    throw new IOException("Avro type " + s.getType().getName() + " not supported in Thrift.");
  }

  protected String getFieldDefault(Schema s, JsonNode value) throws IOException {
    if (value.isNull()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    switch (s.getType()) {
      case ARRAY:
        sb.append("[");
        Iterator<JsonNode> arrayIter = value.getElements();
        while (arrayIter.hasNext()) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          sb.append(getFieldDefault(s.getElementType(), arrayIter.next()));
        }
        return sb.append("]").toString();
      case MAP:
        sb.append("{");
        Iterator<Map.Entry<String, JsonNode>> mapIter = value.getFields();
        while (mapIter.hasNext()) {
          if (first) {
            first = false;
          } else {
            sb.append(", ");
          }
          Map.Entry<String, JsonNode> e = mapIter.next();
          sb.append(e.getKey()).append(':').append(getFieldDefault(s.getValueType(), e.getValue()));
        }
        return sb.append("}").toString();
      case ENUM:
        return sb.append(s.getName()).append('.').append(value.getTextValue()).toString();
      case BOOLEAN:
        return sb.append(value.asBoolean()).toString();
      case DOUBLE:
      case FLOAT:
        return sb.append(value.asDouble()).toString();
      case INT:
        return sb.append(value.asInt()).toString();
      case LONG:
        return sb.append(value.asLong()).toString();
      case STRING:
        return objectMapper.writeValueAsString(value);
    }
    throw new IOException("Avro default value unsupported in Thrift: " + value.toString());

  }
}
