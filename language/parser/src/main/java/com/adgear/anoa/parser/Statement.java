package com.adgear.anoa.parser;

import com.adgear.anoa.parser.state.EnumState;
import com.adgear.anoa.parser.state.Field;
import com.adgear.anoa.parser.state.State;
import com.adgear.anoa.parser.state.StructState;
import com.adgear.anoa.parser.type.FieldType;
import com.adgear.anoa.parser.type.TypeFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final public class Statement {

  final public String origin;
  final public int line;
  final public StatementType statementType;
  final public boolean dependsOnEnum;
  final public boolean dependsOnStruct;
  final public Optional<String> structFieldModifier;
  final public Optional<String> structFieldType;
  final public Optional<String> structFieldDefault;

  final Optional<String> literal;
  final String docstring;
  final Optional<String> fieldReference;

  private Statement(String origin,
                    int line,
                    StatementType statementType,
                    Function<String, Optional<String>> matchGroup) {
    this.origin = origin;
    this.line = line;
    this.statementType = statementType;
    literal = matchGroup.apply("literal");
    fieldReference = matchGroup.apply("reference").filter(s -> !s.equals("_"));
    docstring = matchGroup.apply("docstring")
        .map(Statement::extractDocString).orElse("");
    structFieldDefault = matchGroup.apply("default");
    structFieldModifier = matchGroup.apply("modifier");
    structFieldType = matchGroup.apply("type");
    String type = structFieldType.orElse("").toUpperCase();
    dependsOnEnum = type.endsWith(".ENUM");
    dependsOnStruct = type.endsWith(".STRUCT");
  }

  static public Statement tokenize(String origin, int lineNumber, String statement) {
    String trimmed = statement.trim();
    for (StatementType candidate : StatementType.values()) {
      Matcher matcher = candidate.pattern.matcher(trimmed);
      if (matcher.matches()) {
        return new Statement(origin, lineNumber, candidate, groupName -> {
          try {
            return Optional.ofNullable(matcher.group(groupName));
          } catch (IllegalArgumentException e) {
            return Optional.empty();
          }
        });
      }
    }
    throw new AnoaSyntaxException(origin, lineNumber, statement);
  }

  static private String extractDocString(String quotedDocString) {
    try {
      return TypeFactory.toJson(quotedDocString).getTextValue();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public UnaryOperator<EnumState> parseEnum() {
    switch (statementType) {
      case STRUCT_FIELD_CREATE:
        throw new AnoaParseException(this, "cannot create struct field in enum");
      case ENUM_FIELD_CREATE:
        return e -> e.addEnumField(literal.get(), docstring);
      default:
        return common();
    }
  }

  public UnaryOperator<StructState> parseStruct(EnumState enumDependency,
                                               StructState structDependency) {
    switch (statementType) {
      case STRUCT_FIELD_CREATE:
        final FieldType fieldType;
        if (dependsOnEnum) {
          fieldType = TypeFactory.enumType(this, enumDependency);
        } else if (dependsOnStruct) {
          fieldType = TypeFactory.structType(this, structDependency);
        } else {
          fieldType = TypeFactory.primitiveType(this);
        }
        return r -> r.addStructField(literal.get(), docstring, fieldType);
      case ENUM_FIELD_CREATE:
        throw new AnoaParseException(this, "cannot create enum field in struct declaration");
      default:
        return common();
    }
  }

  private <F extends Field<F>, R extends State<F, R>> UnaryOperator<R> common() {
    switch (statementType) {
      case DEPRECATE:
        return fieldReference.isPresent()
               ? s -> s.doChild(this, fieldReference.get(), f -> f.doDeprecate(this))
               : s -> s.doDeprecate(this);
      case REMOVE:
        return fieldReference.isPresent()
               ? s -> s.doChild(this, fieldReference.get(), f -> f.doRemove(this))
               : s -> s.doRemove(this);
      case RESTORE:
        return fieldReference.isPresent()
               ? s -> s.doChild(this, fieldReference.get(), f -> f.doRestore(this))
               : s -> s.doRestore(this);
      case RENAME:
        return fieldReference.isPresent()
               ? s -> s.doChild(this, fieldReference.get(), f -> f.doName(this, literal.get()))
               : s -> s.doName(this, literal.get());
      case REDESCRIBE:
        return fieldReference.isPresent()
               ? s -> s.doChild(this, fieldReference.get(), f -> f.doDoc(this, docstring))
               : s -> s.doDoc(this, docstring);
      default:
        throw new IllegalStateException("Unknown statement type " + statementType);
    }
  }

  static final String WS = "\\s+";
  static final String SEP = "\\s*[\\s,]\\s*";
  static final String QUOTED = "\"(?:[^\"\\\\]|.)*\"";
  static final String FIELD_NAME = "[A-Z]\\w*";
  static final String NAME_PREFIX = "(?:[A-Z][A-Z0-9]*\\.)*";
  static final String SIMPLE_NAME = "[A-Z][A-Z0-9]*(?:_[A-Z][A-Z0-9]*)*";
  static final String NAME_SUFFIX = "\\.(?:ENUM|STRUCT)";
  static final String NAME = NAME_PREFIX + SIMPLE_NAME + NAME_SUFFIX;
  static final String PRIMITIVE = "BOOLEAN|BYTES|DOUBLE|FLOAT|INT|LONG|STRING";

  static final String LIT = "(?<literal>" + FIELD_NAME + ")";
  static final String DOC = "(?<docstring>" + QUOTED + ")";
  static final String MAYBEDOC = "(?:" + SEP + DOC + ")?";
  static final String REF = "(?<reference>(?:_|" + FIELD_NAME + "))";
  static final String TYPE = "(?<modifier>LIST|MAP)?\\s*(?<type>" + PRIMITIVE + "|" + NAME + ")";
  static final String DEFAULT = "(?:" + SEP + "DEFAULT" + WS + "(?<default>" + QUOTED + "|\\S+))?";


  enum StatementType {
    ENUM_FIELD_CREATE   ("VALUE"      + WS + LIT           + MAYBEDOC),
    STRUCT_FIELD_CREATE (TYPE         + WS + LIT + DEFAULT + MAYBEDOC),
    RENAME              ("RENAME"     + WS + REF + SEP + LIT),
    REDESCRIBE          ("REDESCRIBE" + WS + REF + SEP + DOC),
    DEPRECATE           ("DEPRECATE"  + WS + REF),
    REMOVE              ("REMOVE"     + WS + REF),
    RESTORE             ("RESTORE"    + WS + REF);

    final public Pattern pattern;

    StatementType(String regex) {
      this.pattern = Pattern.compile(regex + ".*", Pattern.CASE_INSENSITIVE);
    }
  }
}
