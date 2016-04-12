# anoa

Anoa is a Java 8 library, language, compiler and Maven plugin for accessing and serializing
structured data with [Avro](https://avro.apache.org),
[Protobuf](https://developers.google.com/protocol-buffers/), [Thrift](https://thrift.apache.org) and
[Jackson](https://github.com/FasterXML/jackson) in a sane and consistent manner.

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
  * The Anoa Maven plugin compiles these schemata into correct Java source code, and generates
    Java interfaces for working with these Avro records, Protobuf messages and Thrift structs.
  * The Anoa library provides facilities for working with these Java objects, namely factories for
    generating de/serializers with one method call. No more writing error-prone badly-understood
    boilerplate. Exception-handling facilities are provided for dealing with broken data in batch
    de/serialization.
  * Last but not least the Anoa tools provide comprehensive Jackson support. This allows conversion
    of an Avro, Protobuf or Thrift object to and from a natural representation as a JSON object.

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

#### Schema definition names

Enums and structs have names and aliases which also obey the lowercase-underscore convention. Enums
are distinguished from structs in that they list their members between square brackets instead of
curly braces. They both may have aliases, which if not qualified are assumed to belong to the
current namespace.

    Name                 ::=  Identifier
    Alias                ::=  ','? ( Identifier | QualifiedIdentifier )
    QualifiedIdentifier  ::=  ( Identifier '.' )+ Identifier
    Identifier           ::=  ['a'-'z'] ( '_' ['a'-'z'] | ['a'-'z''0'-'9'] )*

#### Enumerations

Enum symbols obey the uppercase-underscore convention, with the first ordinal being 0, as in java.
Each enum symbol must be unique within the namespace (i.e. the file).

    EnumSymbolDefinition ::=  EnumSymbol ( ',' | ';' ) ?
    EnumSymbol           ::=  ['A'-'Z'] ( '_'? ['A'-'Z''0'-'9'] )*

#### Structure fields

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
    FieldType            ::=  ReferenceType | SeqType | MapType | PrimitiveType
    FieldName            ::=  Identifier
    FieldAlias           ::=  ','? Identifier


#### Field properties

Fields can be tagged with custom properties, as in Avro. Property keys can be set to an explicit
primitive JSON value. If not explicit, then the value is `true`. The anoa compiler recognizes only
two property keys: `deprecated` and `removed`. For instance, a field decorated with `@deprecated` or
`@deprecated(true)`, which is equivalent, will be marked as deprecated in its Avro and Protobuf
schemata. Decorating it with `@removed` will remove it from the Avro schema and declare the field
tag number as `reserved` in the Protobuf schema. This helps deal with the rule that a field
definition may not be deleted.

    FieldProperty        ::=  '@' PropertyKey ( '(' PropertyValue ')' )?
    PropertyKey          ::=  Identifier
    PropertyValue        ::=  BooleanLiteral | IntegerLiteral | FloatLiteral | StringLiteral

#### Field types

A field's type can be another struct or enum, referred to by an identifier. If the identifier is
unqualified, it must refer to a previous declaration in the same file. If it's qualified, the
corresponding namespace will be imported. Circular dependencies are not allowed. The default value
of a struct field is the default struct and the default value of an enum field is the first enum
symbol of that type, as in Protobuf 3.

    ReferenceType        ::=  QualifiedIdentifier | Identifier | '`' Identifier '`'

A field's type can also be a list or a set or a map of values, whose type is either a struct, or an
enum, or a primitive type. The field's default value is the empty collection, as in Protobuf 3. At
this time, only `string` is allowed as a map key type. Sets and Maps are sorted by key.

    SeqType              ::=  ( 'list' | 'set' ) '<' ValueType '>'
    MapType              ::=  'map' '<' MapKeyType ',' ValueType '>'
    MapKeyType           ::=  'string'
    ValueType            ::=  ReferenceType | PrimitiveValueType
    PrimitiveValueType   ::=  'boolean' | 'bytes' | 'string' | IntegerType | RationalType

#### Primitive field types and default values

Finally, a field's type can be a primitive type. In this case, the field can also specify a custom
default value. This custom default value is considered to be part of the field's type: for example,
`string("foo")` is a type, and `string("bar")` is another, different type. The standard default
values are the same ones as in Protobuf 3: zero for numerical types, `false` for the boolean, and
the empty string for the string types. Consequently, the type `boolean` is equal to the type
`boolean(false)`, and likewise `integer[,]`, `rational[,,]`, `bytes`, `string` are equal to
`integer[,](0)`, `rational[,,](0x0p0)`, `bytes()` and `string("")`, respectively.

    PrimitiveType        ::=    'boolean'              ( '(' BooleanLiteral ')' )?
                              | 'bytes'                ( '(' BytesLiteral   ')' )?
                              | IntegerType            ( '(' IntegerLiteral ')' )?
                              | RationalType           ( '(' FloatLiteral   ')' )?
                              | 'string'               ( '(' StringLiteral  ')' )?

#### Numerical types

Anoa requests that numerical types specify range and precision information.

    IntegerType          ::=   'sint8' | 'uint8' | 'sint16' | 'uint16' | 'sint32' | 'uint32'
                              | ( 'integer' IntegerPrecision )
    RationalType         ::=   'float64'
                              | ( 'rational' '[' RatBound ',' RatBound ',' Mantissa ']' )
    IntegerPrecision     ::=  '[' IntegerLiteral? ',' IntegerLiteral? ']'
    RationalPrecision    ::=  '[' FloatLiteral? ',' FloatLiteral? ',' IntegerLiteral? ']'

The first and second literals are the lower and upper bounds, respectively. They default to `-2^63`
and `2^63-1` in the integral case, and `-0x1.fffffffffffffP+1023` and `0x1.fffffffffffffP+1023`
in the rational case. The third literal in the rational precision information is the number of bits
required for representing the mantissa. If not specified, this corresponds to 53, the setting for
IEEE 754 doubles.

Aliases are provided for the more common numerical types, `float64` is `rational[,,53]` and

    sint8    =  integer  [          -128, 127          ]
    uint8    =  integer  [             0, 256          ]
    sint16   =  integer  [       -32'768, 32'767       ]
    uint16   =  integer  [             0, 65'536       ]
    sint32   =  integer  [-2'147'483'648, 2'147'483'647]
    uint32   =  integer  [             0, 4'294'967'296]

#### Literals

Beyond the `Identifier` and `EnumSymbol` literals presented above, the anoa language supports the
usual primitive value literals. Boolean and text value literals are the same as in JSON and strings
obey the same escaping rules. Byte strings however are represented as a sequence of integer value
literals, which must be in the range `[ 0x00, 0xff ]`. Numeric literals are represented the same way
as in Java in base 10 or in base 16. Integers can also be represented in base 8, and floats also
accept the tokens `NaN` and `Infinity`.

    BooleanLiteral       ::=  'true' | 'false'
    BytesLiteral         ::=  IntegerLiteral+
    FloatLiteral         ::=  <java_float_literal>
    IntegerLiteral       ::=  <java_integer_literal>
    StringLiteral        ::=  <json_string_literal>

### Schema generation

Avro, Protobuf and Thrift schema generation obeys the principle of least surprise and produces
pretty much what you'd expect. A few things to note:

  * Although Anoa requires Protobuf 3, the generated language level is `proto2` to allow custom
    default values.
  * Integer types `int` and `long` are mapped to `sint32` and `sint64` in Protobuf.
  * Floating point type `float` is mapped to `double` in Thrift.

### Avro, Protobuf and Thrift Java source generation

Subsequent Java code generation is also pretty straightforward and is handled by the Anoa maven
plugin:

  * Avro SpecificRecord implementations are generated with "Avro" appended to the struct or enum
    name. For example, the struct `bid_request` in the `open_rtb` namespace becomes
    `open_rtb.BidRequestAvro`. The generated code is a drop-in replacement to that which would have
    been generated by Avro's `SpecificCompiler` invoked by `avro-maven-plugin`.
  * Protobuf protocols are generated with "Protobuf" appended to the name: `open_rtb` becomes
    the class `OpenRtbProtobuf` and `bid_request` becomes its subclass `BidRequest`. Code generation
    occurs by invoking the command-line `protoc` compiler.
  * Thrift TBase implementations are generated with "Thrift" appended to the struct or enum name,
    thus `bid_request` becomes `open_rtb.BidRequestThrift`. Code generation occurs by invoking the
    command-line `thrift` compiler.
  * Unfortunately the Thrift compiler is buggy and `binary` thrift types with custom default values
    are broken. The Anoa plugin repairs the broken java code before it is compiled.

Plugin configuration settings which are of note:

  * `generateAvro`, `generateProtobuf` and `generateThrift` set whether Java code is generated
    along with the schemas or not. These are set to `true` by default.
  * `protocCommand` and `thriftCommand` set the command for invoking the Protobuf and Thrift
    compilers, and are respectively set to `protoc` and `thrift` by default.

### Interface generation

In general, the above objects would typically find their way into your Java applications' business
logic, for example in bolts within a Storm topology or in mappers and reducers in a Hadoop job. We
find it desirable to decouple the business logic from the underlying object representation, which is
why the Anoa compiler also generates a Java interface for each enum and struct for building and
accessing those in an representationally-agnostic manner.

Consider for example the `simple` struct defined in `com.adgear.anoa.test`:

    simple {
      1: integer[,] foo;
      2: bytes bar;
      3: rational[,,] baz;
    }

The compiler will generate an interface `com.adgear.anoa.test.Simple` that looks somewhat like this:

    public interface Simple<T> extends Supplier<T>, Serializable {

      long getFoo();
      boolean isDefaultFoo();

      Supplier<byte[]> getBar();
      boolean isDefaultBar();

      double getBaz();
      boolean isDefaultBaz();
    }

The underlying implementations are serializable and the accessors are guaranteed to return immutable
objects. Protobuf does this already, unfortunately Avro and Thrift don't and we've found this to be
the source of countless subtle bugs. We try to make the underlying code as fast as possible while
remaining correct.

The Avro implementation for this example will be `com.adgear.anoa.test.SimpleAvro`. This class will
also implement `org.apache.avro.specific.SpecificRecord` and thus `get()` will return `this`. The
Protobuf and Thrift implementations will be `com.adgear.anoa.test.Simple.Protobuf` and
`com.adgear.anoa.test.Simple.Thrift` respectively. All implementations define static builder methods
`avro`, `protobuf` and `thrift` for the desired underlying representation.

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
