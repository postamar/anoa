# anoa

A Java 1.7 library for robust record iteration, loosely centered on the Apache Avro system.

## Summary

The heart of the library consists of implementations of the following four interfaces.

    com.adgear.anoa.provider.Provider
    com.adgear.anoa.source.Source
    com.adgear.anoa.codec.Codec
    com.adgear.anoa.sink.Sink

`Provider` extends `Iterable` and `Iterator`, and the general principle in Anoa is that `next()`
handles failures by returning `null` instead of raising an exception. All the while, the `Provider`
logs an appropriate warning and increments an appropriate counter, leaving any error handling to be
performed at the end of the iteration. This allows for a variable level of fault-tolerance. At
AdGear, we find this especially useful when processing raw event logs in a Hadoop map-reduce job,
for instance.

`Source` extends `Provider` and `Closeable`, and is intended for iterating over data sources. For
instance, a `StringLineSource` will iterate over the lines in a text file: `next()` will return a
`String`, or `null` if the stream closes unexpectedly. `Codec` also extends `Provider` but is
intended for iterating over transformed records from another `Provider` instance. Finally, a
`Sink` is intended for collecting records from a `Provider`, typically into a file.

See the javadocs for further details.

## Supported formats / systems

Currently, implementations exist to read from and write to the following formats / systems:

    Apache Avro 1.7.4
    Apache Thrift 0.9.1
    Jackson 2.1.4
    MessagePack 0.6.8
    CSV & TSV (via SuperCSV)
    JDBC ResultSet (read only)

## License

Released under the Apache License, Version 2.0. See LICENSE file for details.
Copyright (C) 2013-2014, Marius Posta.