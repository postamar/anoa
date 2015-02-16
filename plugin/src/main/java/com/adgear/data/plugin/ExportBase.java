package com.adgear.data.plugin;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for exporting an Avro schema in another data declaration format, such as Thrift.
 */
abstract public class ExportBase {

  static final protected Pattern idPattern =
      Pattern.compile("^\\s*\\[([0-9]+)\\]", Pattern.MULTILINE);

  static public List<Schema> collectDependencies(Schema root,
                                                 List<Schema> list,
                                                 boolean includeOnly) {
    switch (root.getType()) {
      case ENUM:
        if (!list.contains(root)) {
          list.add(root);
        }
        break;
      case UNION:
        for (Schema s : root.getTypes()) {
          collectDependencies(s, list, includeOnly);
        }
        break;
      case MAP:
        collectDependencies(root.getValueType(), list, includeOnly);
        break;
      case ARRAY:
        collectDependencies(root.getElementType(), list, includeOnly);
        break;
      case RECORD:
        if (!includeOnly) {
          for (Schema.Field field : root.getFields()) {
            collectDependencies(field.schema(), list, false);
          }
        }
        if (!list.contains(root)) {
          list.add(root);
        }
        break;
      default:
    }
    return list;
  }

  public String exportEnum(Schema s) throws IOException {
    StringBuilder sb = new StringBuilder();
    buildNameSpace(s, sb);
    sb.append('\n');
    buildEnumHeader(s, sb);
    int pos = 0;
    for (String symbol : s.getEnumSymbols()) {
      buildEnumField(symbol, pos++, sb);
    }
    buildEnumFooter(sb);
    return sb.toString();
  }

  public String exportRecord(Schema s, Map<String, String> includePaths, Map<String, String> names)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    buildNameSpace(s, sb);
    sb.append('\n');
    List<Schema> includeList = new ArrayList<Schema>();
    for (Schema.Field field : s.getFields()) {
      collectDependencies(field.schema(), includeList, true);
    }
    for (Schema include : includeList) {
      buildInclude(include, includePaths, sb);
    }
    sb.append('\n');

    buildRecordHeader(s, sb);
    int pos = 0;
    for (Schema.Field field : s.getFields()) {
      if (field.doc() == null) {
        throw new IOException(String.format("Missing docstring for field %s in record %s.",
                                            field.name(), s.getFullName()));
      }
      Matcher idMatcher = idPattern.matcher(field.doc());
      if (!idMatcher.find()) {
        throw new IOException(String.format("Bad docstring for field %s in record %s.",
                                            field.name(), s.getFullName()));
      }
      final String id = idMatcher.group(1);
      buildRecordField(id, field, names, pos++, sb);
    }
    buildRecordFooter(sb);
    return sb.toString();
  }


  abstract protected void buildNameSpace(Schema s, StringBuilder sb);

  abstract protected void buildInclude(Schema s,
                                       Map<String, String> includePaths,
                                       StringBuilder sb);

  abstract protected void buildEnumHeader(Schema s, StringBuilder sb);

  abstract protected void buildEnumFooter(StringBuilder sb);

  abstract protected void buildEnumField(String symbol, int pos, StringBuilder sb);

  abstract protected void buildRecordHeader(Schema s, StringBuilder sb);

  abstract protected void buildRecordFooter(StringBuilder sb);

  abstract protected void buildRecordField(String id,
                                           Schema.Field field,
                                           Map<String, String> names,
                                           int pos,
                                           StringBuilder sb) throws IOException;

  final protected Schema resolveSchema(Schema s) throws IOException {
    if (s.getType() == Schema.Type.UNION) {
      if (s.getTypes().size() == 2) {
        if (s.getTypes().get(0).getType() == Schema.Type.NULL) {
          return s.getTypes().get(1);
        } else if (s.getTypes().get(1).getType() == Schema.Type.NULL) {
          return s.getTypes().get(0);
        }
      }
      throw new IOException("Complex unions are not supported.");
    }
    return s;
  }
}
