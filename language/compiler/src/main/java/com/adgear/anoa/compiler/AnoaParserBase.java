package com.adgear.anoa.compiler;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringEscapeUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.LongNode;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Superclass for javacc-generated parser class AnoaParser.
 */
abstract public class AnoaParserBase implements Closeable {

  /* STATIC MEMBERS */

  /**
   * Schema.Field property key for field ordinal value.
   */
  static public final String ORDINAL_PROP_KEY = "ordinal_";


  /**
   * Schema property key for type modifiers.
   */
  static public final String SORTED_PROP_KEY = "sorted";
  static public final String SET_PROP_KEY = "set";

  static public final String LOWER_BOUND_PROP_KEY = "min";
  static public final String UPPER_BOUND_PROP_KEY = "max";
  static public final String MANTISSA_BITS_PROP_KEY = "mantissa";
  static public final String UNSIGNED_PROP_KEY = "unsigned";


  /**
   * Stores last-seen docstring.
   */
  static final private ThreadLocal<String> DOC = new ThreadLocal<>();

  /**
   * Stores all known compilation units.
   */
  static final private Map<String, Protocol> COMPILATION_UNITS = new ConcurrentHashMap<>();


  /* STATIC METHODS */

  static protected void setDoc(String doc) {
    DOC.set(doc.trim());
  }

  static protected String getDoc() {
    String doc = DOC.get();
    DOC.set(null);
    return doc;
  }

  /**
   * Builds URL to anoa namespace definition file or resource
   */
  static protected URL toURL(String namespace, File baseDir, ClassLoader resourceLoader)
      throws IOException {
    String path = namespace.replace('.', File.separatorChar) + ".anoa";
    File completeFile = (baseDir == null) ? new File(path) : new File(baseDir, path);
    if (completeFile.exists()) {
      return completeFile.toURI().toURL();
    } else if (resourceLoader != null) {
      URL url = resourceLoader.getResource(path);
      if (url != null) {
        return url;
      }
    }
    throw new FileNotFoundException(path);
  }

  /* MEMBERS */

  final protected List<String> ancestry = new ArrayList<>();

  final private Map<Schema,Boolean> types = new LinkedHashMap<>();
  final private List<String> enumSymbols = new ArrayList<>();
  final private List<String> imports = new ArrayList<>();

  /* PACKAGE-LOCAL METHODS */

  Stream<String> getImportedNamespaces() {
    return imports.stream();
  }

  /* ABSTRACT METHODS IMPLEMENTED AND INVOKED BY SUBCLASS  */

  /**
   * @return namespace being processed by this instance
   */
  abstract String getNamespace();

  /**
   * @param namespace namespace to import for the current compilation unit
   * @return imported compilation unit
   * @throws IOException error reading imported namespace definition
   * @throws ParseException parse error in imported namespace
   */
  abstract protected Protocol parse(String namespace) throws IOException, ParseException;

  /* PROTECTED METHODS INVOKED BY SUBCLASS */

  protected Protocol exportProtocol() {
    String name = capitalizeQualified(getNamespace());
    name = name.substring(name.lastIndexOf('.') + 1);
    Protocol p = new Protocol(name, getNamespace());
    List<Schema> exportedTypes = new ArrayList<>();
    types.forEach((type, imported) -> {
      if (Boolean.FALSE.equals(imported)) {
        exportedTypes.add(type);
      }
    });
    p.setTypes(exportedTypes);
    return p;
  }

  protected String getTypeName(Token typeName) throws ParseException {
    String fullName = buildFullName(typeName.image);
    if (getByFullName(fullName).isPresent()) {
      throw error("Type name collision", typeName);
    }
    return fullName;
  }

  protected void addTypeAlias(Token typeAlias, String typeName, List<String> aliases)
      throws ParseException {
    String fullName = buildFullName(typeAlias.image);
    if (typeName.equals(fullName)) {
      throw error("Type alias is identical to type name", typeAlias);
    }
    if (aliases.contains(fullName)) {
      throw error("Repeated type alias", typeAlias);
    }
    if (getByFullName(fullName).isPresent()) {
      throw error("Type alias name collision", typeAlias);
    }
    aliases.add(fullName);
  }

  protected void addType(Schema type, List<String> aliases) {
    aliases.forEach(alias -> {
      int index = alias.lastIndexOf('.');
      type.addAlias(alias.substring(index + 1), alias.substring(0, index));
    });
    types.put(type, false);
  }

  protected void addEnumSymbol(Token enumSymbol, List<String> enumSymbols) throws ParseException {
    String symbol = enumSymbol.image;
    if (enumSymbols.contains(symbol)) {
      throw error("Repeated enum symbol", enumSymbol);
    }
    if (this.enumSymbols.contains(symbol)) {
      throw error("Enum symbol name collision", enumSymbol);
    }
    enumSymbols.add(symbol);
    this.enumSymbols.add(symbol);
  }

  protected void addField(List<Schema.Field> fields,
                          Token ordinal,
                          String fieldName,
                          Schema.Field auxField)
      throws ParseException {
    long version = 0L;
    if (!fields.isEmpty()) {
      version = fields.get(fields.size() - 1).getJsonProp(ORDINAL_PROP_KEY).asLong();
    }
    long fieldOrdinal = getIntegerLiteral(ordinal);
    if (fieldOrdinal <= 0L || fieldOrdinal > 536870911L) {
      throw error("Field ordinal is out of range, must be positive and less than 2^29", ordinal);
    }
    if (fieldOrdinal <= version) {
      throw error("Invalid field ordinal, should be greater than " + version, ordinal);
    }
    Schema.Field field = new Schema.Field(fieldName,
                                          auxField.schema(),
                                          auxField.doc(),
                                          auxField.defaultValue());
    field.addProp(ORDINAL_PROP_KEY, LongNode.valueOf(fieldOrdinal));
    for (Map.Entry<String, JsonNode> prop : auxField.getJsonProps().entrySet()) {
      field.addProp(prop.getKey(), prop.getValue());
    }
    auxField.aliases().forEach(field::addAlias);
    fields.add(field);
  }

  protected Schema getReferencedType(Token id) throws ParseException {
    String fullName = buildFullName(id.image);
    Schema type = getByFullName(fullName).orElse(null);
    if (null != type) {
      return type;
    }
    String namespace = fullName.substring(0, fullName.lastIndexOf('.'));
    if (namespace.equals(getNamespace())) {
      throw error("Unqualified referenced type not yet declared in current namespace", id);
    }
    List<Schema> types = findByFullName(fullName);
    if (types.isEmpty()) {
      importProtocol(namespace, id);
      types = findByFullName(fullName);
      if (types.isEmpty()) {
        throw error("Referenced type not found in imported namespace", id);
      }
    }
    if (types.size() == 1) {
      return types.get(0);
    }
    throw error("Referenced type is ambiguous, may refer to any of: " + types, id);
  }

  protected String getFieldName(Token fieldName, List<Schema.Field> structFields)
      throws ParseException {
    String name = fieldName.image;
    for (Schema.Field field : structFields) {
      if (name.equals(field.name())) {
        throw error("Field name matches name of previously declared field: " + field, fieldName);
      }
      for (String alias : field.aliases()) {
        if (name.equals(alias)) {
          throw error("Field name matches alias of previously declared field: " + field, fieldName);
        }
      }
    }
    return name;
  }

  protected void addFieldAlias(Schema.Field auxField,
                               Token fieldAlias,
                               String fieldName,
                               List<Schema.Field> structFields)
      throws ParseException {
    String alias = fieldAlias.image;
    if (alias.equals(fieldName)) {
      throw error("Field alias matches name", fieldAlias);
    }
    if (auxField.aliases().contains(alias)) {
      throw error("Field alias matches other alias", fieldAlias);
    }
    for (Schema.Field field : structFields) {
      if (alias.equals(field.name())) {
        throw error("Field alias matches name of previously declared field: " + field, fieldAlias);
      }
      for (String other : field.aliases()) {
        if (alias.equals(other)) {
          throw error("Field alias matches alias of previously declared field: " + field, fieldAlias);
        }
      }
    }
    auxField.addAlias(alias);
  }

  protected long getIntegerLiteral(Token integerLiteral) throws ParseException {
    try {
      return Long.decode(integerLiteral.image);
    } catch (NumberFormatException e) {
      throw error("Invalid integral value: " + e, integerLiteral);
    }
  }

  protected double getFloatLiteral(Token floatLiteral) throws ParseException {
    try {
      return Double.valueOf(floatLiteral.image);
    } catch (NumberFormatException e) {
      throw error("Invalid floating point value: " + e, floatLiteral);
    }
  }

  protected int getByteLiteral(Token byteLiteral) throws ParseException {
    final long byteValue;
    try {
      byteValue = Long.decode(byteLiteral.image);
    } catch (NumberFormatException e) {
      throw error("Byte value out of range: " + e, byteLiteral);
    }
    if (byteValue < 0L || byteValue > 255L) {
      throw error("Byte value out of range", byteLiteral);
    }
    return ((int) byteValue) & 0xff;
  }

  protected String getStringLiteral(Token stringLiteral) {
    String quoted = stringLiteral.image;
    return StringEscapeUtils.unescapeJson(quoted.substring(1, quoted.length() - 1));
  }

  protected Schema buildIntegerType(Long min, Long max, Token token) throws ParseException {
    Schema schema = Schema.create(Schema.Type.LONG);
    if (min != null && max != null) {
      if (min > max) {
        throw error("Invalid range [ " + min + ", " + max + "]", token);
      }
      if (Math.max(Math.max(Math.abs(min), Math.abs(max)), Math.abs(max - min)) < 1L << 32) {
        schema = Schema.create(Schema.Type.INT);
      }
    }
    if (max != null) {
      schema.addProp(UPPER_BOUND_PROP_KEY, LongNode.valueOf(max));
    }
    if (min != null) {
      schema.addProp(LOWER_BOUND_PROP_KEY, LongNode.valueOf(min));
      if (min > 0L) {
        schema.addProp(UNSIGNED_PROP_KEY, BooleanNode.TRUE);
      }
    }
    return schema;
  }

  protected JsonNode getIntegerDefault(long value, Schema type, Token token)
      throws ParseException {
    JsonNode min = type.getJsonProp(LOWER_BOUND_PROP_KEY);
    JsonNode max = type.getJsonProp(UPPER_BOUND_PROP_KEY);
    if (min != null && value < min.getLongValue()) {
      throw error("Default value " + value + " is lesser than stated lower bound " + min, token);
    }
    if (max != null && max.getLongValue() < value) {
      throw error("Default value " + value + " is greater than stated upper bound " + max, token);
    }
    return LongNode.valueOf(value);
  }

  protected Schema buildRationalType(Number min, Number max, Long mantissa, Token token)
      throws ParseException {
    Schema schema = Schema.create((mantissa == null || mantissa > 23)
                                  ? Schema.Type.DOUBLE
                                  : Schema.Type.FLOAT);
    if (max == null) {
      max = Double.POSITIVE_INFINITY;
    } else {
      schema.addProp(UPPER_BOUND_PROP_KEY, DoubleNode.valueOf(max.doubleValue()));
    }
    if (min == null) {
      min = Double.NEGATIVE_INFINITY;
    } else {
      schema.addProp(LOWER_BOUND_PROP_KEY, DoubleNode.valueOf(min.doubleValue()));
    }
    if (max.doubleValue() < min.doubleValue()) {
      throw error("Invalid range [ " + min + ", " + max + "]", token);
    }
    if (mantissa != null) {
      if (mantissa < 0 || mantissa > 54) {
        throw error("Invalid mantissa size, must be in [0, 54]" + mantissa, token);
      }
      schema.addProp(MANTISSA_BITS_PROP_KEY, LongNode.valueOf(mantissa));
    }
    return schema;
  }

  protected JsonNode getRationalDefault(double value, Schema type, Token token)
      throws ParseException {
    JsonNode min = type.getJsonProp(LOWER_BOUND_PROP_KEY);
    JsonNode max = type.getJsonProp(UPPER_BOUND_PROP_KEY);
    if (min != null && value < min.getDoubleValue()) {
      throw error("Default value " + value + " is lesser than stated lower bound " + min, token);
    }
    if (max != null && max.getDoubleValue() < value) {
      throw error("Default value " + value + " is greater than stated upper bound " + max, token);
    }
    return DoubleNode.valueOf(value);
  }

  protected Schema buildCollectionType(Schema elementType, Token token) {
    Schema schema = Schema.createArray(elementType);
    switch (token.image.toUpperCase()) {
      case "SET":
        schema.addProp(SET_PROP_KEY, BooleanNode.TRUE);
        schema.addProp(SORTED_PROP_KEY, BooleanNode.TRUE);
    }
    return schema;
  }

  protected Schema buildMapType(Schema valueType) {
    Schema schema = Schema.createMap(valueType);
    schema.addProp(SORTED_PROP_KEY, BooleanNode.TRUE);
    return schema;
  }

  /* PRIVATE METHODS */

  private ParseException error(String message, Token token) {
    String msg = message + ", line " + token.beginLine + ", column " + token.beginColumn;
    return new ParseException(msg);
  }

  private void importProtocol(String namespace, Token token) throws ParseException {
    if (imports.contains(namespace)) {
      return;
    }
    if (ancestry.contains(namespace)) {
      throw error("Circular dependency: " + ancestry.stream().collect(Collectors.joining(" -> ")),
                  token);
    }
    final Protocol imported;
    try {
      imported = COMPILATION_UNITS.computeIfAbsent(namespace, ns -> {
        try {
          return parse(ns);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    } catch (RuntimeException uncheckedWrapper) {
      String msg = "Error importing namespace '" + namespace +  "' at line " + token.beginLine
                   + ", column " + token.beginColumn + ": " + uncheckedWrapper.getCause();
      throw new ParseException(msg);
    }
    imported.getTypes().forEach(type -> types.put(type, true));
    imports.add(namespace);
  }

  private Optional<Schema> getByFullName(String name) {
    return types.keySet().stream()
        .filter(s -> name.equals(s.getFullName()) || s.getAliases().contains(name))
        .findFirst();
  }

  private List<Schema> findByFullName(String name) {
    List<Schema> found = new ArrayList<>();
    types.keySet().stream()
        .filter(type -> type.getFullName().endsWith(name))
        .forEach(found::add);
    if (found.isEmpty()) {
      types.keySet().stream()
          .filter(type -> type.getAliases().stream().anyMatch(a -> a.endsWith(name)))
          .forEach(found::add);
    }
    return found;
  }

  private String buildFullName(String name) {
    int index = name.lastIndexOf('.');
    return capitalizeQualified(((index < 0) ? (getNamespace() + ".") : "") + name);
  }

  static public String capitalizeQualified(String name) {
    StringBuilder sb = new StringBuilder();
    int index = name.lastIndexOf('.');
    if (index >= 0) {
      sb.append(name.substring(0, index)).append('.');
      name = name.substring(index + 1);
    }
    boolean capitalize = true;
    for (char c : name.toCharArray()) {
      if (c == '_') {
        capitalize = true;
      } else if (capitalize) {
        capitalize = false;
        sb.append(Character.toUpperCase(c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
