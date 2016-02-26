# anoa

Anoa is a Java 8 library built around [Avro](https://avro.apache.org),
[Thrift](https://thrift.apache.org) and [Jackson](https://github.com/FasterXML/jackson) for
de/serializing records in a consistent manner.


## Rationale

At AdGear, we deal with event logs a lot. Events are all over the place, in Kafka
topics, in files, S3 buckets, etc. and they're processed in Hadoop map-reduce jobs, Storm
topologies, command-line tools, scripts of all kinds. And naturally event definitions evolve with
time, so change must be handled gracefully.

We like to use tried-and-proven cross-platform serialization libraries because we hate reinventing
the wheel. Avro is good for batches of records, Thrift is good over the wire and shares a very
similar data model with Avro, JSON is simply ubiquitous and Jackson is its de-facto default parser
in javaland.

Anoa glues these tools together and expose them with a consistent API. Anoa also comes with a
Maven plugin for Avro and Thrift code generation. Finally, Anoa has exception-handling facilities
for dealing with broken data.


## Project Structure

The Anoa project is divided into 4 modules to try to keep dependencies to a minimum:

  * `plugin` is the schema compiler Maven plugin, essentially just a hack of the Avro Maven plugin.
  * `test` defines the entities used in tests of the next two modules.
  * `core` contains the core components of Anoa with a minimal set of dependencies.
  * `tools` is an extension of the above with various tools and extended Jackson support.


### Plugin and Test

This maven plugin consumes `*.avdl` schema definition files and generates `*.avsc` Avro schema
files, `*.thrift` Thrift schema files, as well as the corresponding code: `SpecificRecord` objects
for Avro, `TBase` objects for thrift.

The definition language is the Avro [IDL](https://avro.apache.org/docs/1.7.7/idl.html) with a few
modifications notably for Thrift interop.

Restrictions:

  * No protocols.
  * Only simple unions with null are allowed, i.e. `union {null, <SUBTYPE>}`.

Enhancements:

  * Java-style imports with `require` block in top-level comment string.
  * Thrift field index declaration with `/** [<THRIFTIDX>] <DOCSTRING> */` in field comment.
  * Thrift integer subtypes with `@thrift(<SUBTYPE>)` field annotations.

The `test` module contains the schema definitions for the records used by tests in `core` and
`tools`. Assuch, consider it a reference for `plugin` usage.
Comprehensive examples of Anoa-flavored IDL syntax are available
[here](https://github.com/postamar/anoa/tree/master/test/src/main/avro/com/adgear/avro).


### Core

The Anoa core API can be divided into three general categories:

  * reading serialized objects: public methods and classes in package `com.adgear.anoa.read`,
  * writing serialized objects: public methods and classes in package `com.adgear.anoa.write`,
  * exception handling: public methods and classes in package `com.adgear.anoa`.

See the [anoa-core javadoc](http://www.javadoc.io/doc/com.adgear/anoa-core) for more details.


### Tools

We made sure to include only minimal dependencies in `core`, for this reason this module contains
all the stuff that didn't fit in there, and more:

  * JDBC support,
  * extended Jackson support: CSV, CBOR, Smile, etc.
  * a few utility functions for operations on records, in `com.adgear.anoa.tools.function`,
  * a few command-line tools, in `com.adgear.anoa.tools.runnable`.

See the [anoa-tools javadoc](http://www.javadoc.io/doc/com.adgear/anoa-tools) for more details.


## Version Info

Anoa is currently built with:

    Apache Avro 1.7.7
    Apache Thrift 0.9.2
    FasterXML Jackson 2.7.1

We're also slowly adding support for Google Protobufs, version 2.6.1.

## License

Released under the Apache License, Version 2.0. See LICENSE file for details.
Copyright (C) 2013-2016, Marius Posta.
