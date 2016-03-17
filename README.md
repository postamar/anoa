# anoa

Anoa is a Java 8 library, language, compiler and Maven plugin for serializing structured data with
[Avro](https://avro.apache.org), [Protobuf](https://developers.google.com/protocol-buffers/),
[Thrift](https://thrift.apache.org) and [Jackson](https://github.com/FasterXML/jackson) in a sane
and consistent manner.

## Rationale

At AdGear, we deal with event logs a lot. Events are all over the place, in Kafka
topics, in files, S3 buckets, etc. and they're processed in Hadoop map-reduce jobs, Storm
topologies, command-line tools, scripts of all kinds. And naturally event definitions evolve with
time, so change must be handled gracefully.

We like to use tried-and-proven cross-platform serialization libraries because we hate reinventing
the wheel. Avro is good for batched data and long-term storage, Protobuf is good over the wire, JSON
is simply ubiquitous and Jackson is its de-facto default parser in javaland. Thrift also exists.

Anoa glues these tools together and exposes them with a consistent API:

  * Event schemata are defined using the Anoa language.
  * The Anoa compiler transpiles these definitions to semantically equivalent Avro, Protobuf and
    Thrift schemata; i.e. `.avpr`, `.proto` and `.thrift` files.
  * The Anoa Maven plugin compiles these schemata into correct Java source code.
  * The Anoa library provides facilities for working with these Java objects, namely factories for
    generating de/serializers with one method call. No more writing error-prone badly-understood
    boilerplate. Exception-handling facilities are provided for dealing with broken data in batch
    de/serialization.
  * Last but not least the Anoa tools provide comprehensive Jackson support. This allows conversion
    of an Avro, Protobuf or Thrift object to and from a natural representation as a JSON object as
    long as the schema allows it.

## Project Structure

The Anoa project is divided into 7 Maven modules to try to keep dependencies to a minimum:

  * `language` contains the 3 submodules related to the IDL, compiler and Maven plugin:
    * `compiler` has code generation code and a javacc parser definition;
    * `plugin` exposes the above as a Maven plugin;
    * `test` applies the plugin to some Anoa IDL files.
  * `test` extends `language-test` to define the entities used in tests in the last two modules.
  * `core` contains the core components of the Anoa library with a minimal set of dependencies.
  * `tools` is an extension of the above with various tools and extended Jackson support.

## Language

Compared to Protobuf version 1 or 2, or to Avro, the Anoa language may seem needlessly restrictive
in terms of its expressivity, but experience has brought us to the same conclusions as those arrived
to by the maintainers of Protobuf 3, namely that required fields and default values are bad for
schema evolution. What it comes down to is you can't remove required fields once you've added them,
and altering default values has all sorts of unintended consequences.

### Namespaces

An *anoa file* can contain multiple *schema definitions*. Each *schema definitions* is either that
of a *structure* or of an *enumeration*. These definitions are scoped to a *namespace* inferred by
the path of the file from the *anoa root directory*, similar to java source code.

This is best described by example. Consider `language/test/src/anoa` to be the anoa root directory
for this example. The files `openrtb.anoa` and `com.adgear.anoa.test.ad_exchange.anoa` respectively
define the `open_rtb` and the `com.adgear.anoa.test.ad_exchange` namespaces. The latter file has two
schema definitions: an enumeration named `log_event_type` and a structure named `log_event`.

All file paths, and hence namespaces, should follow lowercase-underscore naming conventions, much
like java namespaces/packages.

### Grammar

Every anoa file contains at least one schema definition, as explained above.

    AnoaFile             ::=  SchemaDefinition+
    SchemaDefinition     ::=  EnumDefinition | StructDefinition
    EnumDefinition       ::=  Name Alias* '[' EnumSymbolDefinition+ ']'
    StructDefinition     ::=  Name Alias* '{' FieldDefinition+ '}'

Enums and structs have names and aliases which also obey the lowercase-underscore convention. Enums
are distinguished from structs in that they list their members between square brackets instead of
curly braces. They both may have aliases, which if not qualified are assumed to belong to the
current namespace.

    Name                 ::=  Identifier
    Alias                ::=  Identifier | QualifiedIdentifier
    QualifiedIdentifier  ::=  ( Identifier '.' )+ Identifier
    Identifier           ::=  ['a'-'z'] ( '_' ['a'-'z'] | ['a'-'z''0'-'9'] )*

Enum symbols obey the uppercase-underscore convention, with the first ordinal being 0, as in java.
Each enum symbol must be unique within the namespace (i.e. the file).

    EnumSymbolDefinition ::=  EnumSymbol ( ',' | ';' ) ?
    EnumSymbol           ::=  ['A'-'Z'] ( '_' ['A'-'Z'] | ['A'-'Z''0'-'9'] )*

Struct fields are tagged, as in Protobuf and Thrift, and they may be named and aliased as in Avro.
If a name is not provided, it will be auto-generated based on the ordinal. In a struct definition,
field must be defined with increasing ordinal numbers. Furthermore, as in Avro, fields may have
custom properties. Finally, each field is typed.

Schemas evolve over time, and cross-compatibility is ensured provided certain rules are obeyed:
  1. A field definition cannot be deleted.
  2. A field ordinal cannot be altered.
  3. A field type cannot be altered (this includes the default value -- more on that later).
  4. A field name can be altered, as long as the old field name is added as an alias.

If these rules are obeyed, the largest ordinal can be considered to be the *version number* of the
struct.

    FieldDefinition      ::=  FieldOrdinal ':' FieldType FieldProperty* FieldName? FieldAlias* ';'
    FieldOrdinal         ::=  IntegerLiteral
    FieldType            ::=  ReferenceType | ListType | MapType | PrimitiveType
    FieldProperty        ::=  '@' PropertyKey ( '(' PropertyValue ')' )?
    PropertyKey          ::=  Identifier
    PropertyValue        ::=  BooleanLiteral | IntegerLiteral | FloatLiteral | StringLiteral
    FieldName            ::=  Identifier
    FieldAlias           ::=  Identifier

A field's type can be another struct or enum, referred to by an identifier. If the identifier is
unqualified, it must refer to a previous declaration in the same file. If it's qualified, the
corresponding namespace will be imported. Circular dependencies are not allowed. The default value
of a struct field is the default struct and the default value of an enum field is the first enum
symbol of that type, as in Protobuf 3.

    ReferenceType        ::=  QualifiedIdentifier | Identifier | '`' Identifier '`'

A field's type can also be a list or a map of values, whose type is either a struct, or an enum, or
a primitive type. The field's default value is the empty collection, as in Protobuf 3. At this time,
only `string` is allowed as a map key type.

    ListType             ::=  'list' '<' ValueType '>'
    MapType              ::=  'map' '<' MapKeyType ',' ValueType '>'
    MapKeyType           ::=  'string'
    ValueType            ::=  ReferenceType | PrimitiveValueType
    PrimitiveValueType   ::=  'boolean' | 'bytes' | 'string' | 'int' | 'long' | 'float' | 'double'

Finally, a field's type can be a primitive type. In this case, the field can also specify a custom
default value. This custom default value is considered to be part of the field's type: for example,
`int(1)` is a type, and `int(2)` is another, different type. The standard default values are the
same ones as in Protobuf 3: 0 for numerical types, `false` for the boolean, and the empty string for
the string types. Consequently, the type `boolean` is equal to the type `boolean(false)`, and
likewise `int`, `long`, `float`, `double`, `bytes`, `string` are equal to `int(0)`, `long(0L)`,
`float(0x0p0)`, `double(0x0p0)`, `bytes()` and `string("")`, respectively.

    PrimitiveType        ::=    'boolean'              ( '(' BooleanLiteral ')' )?
                              | 'bytes'                ( '(' BytesLiteral   ')' )?
                              | ( 'int' | 'long' )     ( '(' IntegerLiteral ')' )?
                              | ( 'float' | 'double' ) ( '(' FloatLiteral   ')' )?
                              | 'string'               ( '(' StringLiteral  ')' )?

    BooleanLiteral       ::=  'true' | 'false'
    BytesLiteral         ::=  IntegerLiteral+
    FloatLiteral         ::=  <java_float_literal>
    IntegerLiteral       ::=  <java_integer_literal>
    StringLiteral        ::=  <json_string_literal>

### Output

TODO

## Plugin

TODO

## Library

The Anoa core library can be divided into three general categories:

  * reading serialized objects: public methods and classes in package `com.adgear.anoa.read`,
  * writing serialized objects: public methods and classes in package `com.adgear.anoa.write`,
  * exception handling: public methods and classes in package `com.adgear.anoa`.

See the [anoa-core javadoc](http://www.javadoc.io/doc/com.adgear/anoa-core) for more details.

We made sure to include only minimal dependencies in `core`, for this reason the `tools` module
contains all the stuff that didn't fit in there, and more:

  * JDBC support,
  * extended Jackson support: CSV, CBOR, Smile, etc.
  * a few utility functions for operations on records, in `com.adgear.anoa.tools.function`,
  * a few command-line tools, in `com.adgear.anoa.tools.runnable`.

See the [anoa-tools javadoc](http://www.javadoc.io/doc/com.adgear/anoa-tools) for more details.

## Version and build info

Anoa is currently built with:

    Apache Avro 1.7.7
    Google Protobuf 3.0.0-beta-2
    Apache Thrift 0.9.3
    FasterXML Jackson 2.7.3

Building Anoa requires Maven version 3.X. To build without modifying anything, you need the protobuf
compiler `protoc` (version 3.X) and the thrift compiler `thrift` (version 0.9.3) to be invocable as
such on the command line. If they are not on the path on your system, you need to add the
`<protocCommand>` and `<thriftCommand>` configuration settigns to the `<anoa-maven-plugin>`
invocation in `language/test/pom.xml`.

## License

Released under the Apache License, Version 2.0. See LICENSE file for details.
Copyright (C) 2013-2016, Marius Posta.
