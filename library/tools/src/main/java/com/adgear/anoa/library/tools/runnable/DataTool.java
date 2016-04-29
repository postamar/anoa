package com.adgear.anoa.library.tools.runnable;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import com.adgear.anoa.AnoaReflectionUtils;
import com.adgear.anoa.write.AvroConsumers;
import com.adgear.anoa.write.CborConsumers;
import com.adgear.anoa.write.CsvConsumers;
import com.adgear.anoa.write.JacksonConsumers;
import com.adgear.anoa.write.JsonConsumers;
import com.adgear.anoa.write.ProtobufConsumers;
import com.adgear.anoa.write.SmileConsumers;
import com.adgear.anoa.write.ThriftConsumers;
import com.adgear.anoa.write.WriteConsumer;
import com.adgear.anoa.read.AvroDecoders;
import com.adgear.anoa.read.AvroStreams;
import com.adgear.anoa.read.CborStreams;
import com.adgear.anoa.read.CsvStreams;
import com.adgear.anoa.read.JdbcStreams;
import com.adgear.anoa.read.JsonStreams;
import com.adgear.anoa.read.ProtobufDecoders;
import com.adgear.anoa.read.ProtobufStreams;
import com.adgear.anoa.read.SmileStreams;
import com.adgear.anoa.read.ThriftDecoders;
import com.adgear.anoa.read.ThriftStreams;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.jooq.lambda.Unchecked;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataTool<T extends TBase<?, TFieldIdEnum>, M extends Message> implements Runnable {

  final private Schema declaredAvroSchema;
  final private Class<T> thriftClass;
  final private Class<M> protobufClass;
  final private Format inFormat;
  final private Format outFormat;
  final private InputStream in;
  final private OutputStream out;
  final private Connection jdbcConnection;
  final private List<String> jdbcInitStatements;
  final private String jdbcQuery;
  final private int jdbcFetchSize;

  private Schema avroSchema = null;

  /**
   * Constructor for when reading parser JDBC source.
   *
   * @param avroSchema         Declared Avro record schema (optional).
   * @param thriftClass        Declared Thrift record class (optional for some input formats).
   * @param outFormat          Declared output serialization format.
   * @param out                Stream to write records to.
   * @param jdbcConnection     Connection object used to open session.
   * @param jdbcInitStatements Array of individual statements to be executed before query.
   * @param jdbcQuery          SQL query to submit.
   * @param jdbcFetchSize      JDBC result set fetch size.
   */
  public DataTool(Schema avroSchema,
                  Class<T> thriftClass,
                  Class<M> protobufClass,
                  Format outFormat,
                  OutputStream out,
                  Connection jdbcConnection,
                  List<String> jdbcInitStatements,
                  String jdbcQuery,
                  int jdbcFetchSize) {
    this.avroSchema = this.declaredAvroSchema = avroSchema;
    this.thriftClass = thriftClass;
    this.protobufClass = protobufClass;
    this.inFormat = Format.JDBC;
    this.outFormat = outFormat;
    this.in = null;
    this.out = out;
    this.jdbcConnection = jdbcConnection;
    this.jdbcInitStatements = jdbcInitStatements;
    this.jdbcQuery = jdbcQuery;
    this.jdbcFetchSize = jdbcFetchSize;
  }

  /**
   * Constructor for when reading parser stream.
   *
   * @param avroSchema  Declared Avro record schema (optional for some input formats).
   * @param thriftClass Declared Thrift record class (optional for some input formats).
   * @param inFormat    Declared input serialization format.
   * @param outFormat   Declared output serialization format.
   * @param in          Stream to read records parser.
   * @param out         Stream to write records to.
   */
  public DataTool(Schema avroSchema,
                  Class<T> thriftClass,
                  Class<M> protobufClass,
                  Format inFormat,
                  Format outFormat,
                  InputStream in,
                  OutputStream out) {
    this.avroSchema = this.declaredAvroSchema = avroSchema;
    this.thriftClass = thriftClass;
    this.protobufClass = protobufClass;
    this.inFormat = inFormat;
    this.outFormat = outFormat;
    this.in = in;
    this.out = out;
    this.jdbcConnection = null;
    this.jdbcInitStatements = null;
    this.jdbcQuery = null;
    this.jdbcFetchSize = -1;
  }

  static public void main(String[] args) throws Exception {
    final Format inFormat = Format.valueOfIgnoreCase(System.getProperty("inFormat", ""));
    final Format outFormat = Format.valueOfIgnoreCase(System.getProperty("outFormat", ""));
    final String avroClassName = System.getProperty("recordClass", "");
    final String thriftClassName = System.getProperty("thriftClass", "");
    final String protobufClassName = System.getProperty("protobufClass", "");
    final String outFilePath = System.getProperty("out", "");
    final String schemaFilePath = System.getProperty("schemaFilePath", "");
    final String inFilePath = System.getProperty("in", "");

    Schema avroSchema = null;
    if (!avroClassName.isEmpty()) {
      Class<? extends SpecificRecord> avroRecordClass = AnoaReflectionUtils
          .getAvroClass(avroClassName);
      if (avroRecordClass != null) {
        avroSchema = SpecificData.get().getSchema(avroRecordClass);
      }
    }
    Class<? extends TBase> thriftRecordClass = null;
    if (!thriftClassName.isEmpty()) {
      thriftRecordClass = AnoaReflectionUtils.getThriftClass(thriftClassName);
    }
    Class<? extends Message> protobufRecordClass = null;
    if (!protobufClassName.isEmpty()) {
      protobufRecordClass = AnoaReflectionUtils.getProtobufClass(protobufClassName);
    }
    final DataTool instance;
    try (OutputStream out = outFilePath.isEmpty()
                            ? System.out
                            : new FileOutputStream(outFilePath)) {
      if (inFormat == Format.JDBC) {
        final String jdbcUrl = System.getProperty("url");
        final String jdbcQuery = System.getProperty("query");
        final String initPath = System.getProperty("initScript", "");
        Stream<String> initStream = initPath.isEmpty()
                                    ? Stream.<String>empty()
                                    : new BufferedReader(new FileReader(initPath)).lines();
        instance = new DataTool<>(avroSchema,
                                  thriftRecordClass,
                                  protobufRecordClass,
                                  outFormat,
                                  out,
                                  DriverManager.getConnection(jdbcUrl),
                                  initStream.collect(Collectors.toList()),
                                  jdbcQuery,
                                  4096);
      } else {
        InputStream in = inFilePath.isEmpty() ? System.in : new FileInputStream(inFilePath);
        instance = new DataTool<>(avroSchema,
                                  thriftRecordClass,
                                  protobufRecordClass,
                                  inFormat,
                                  outFormat,
                                  in,
                                  out);
      }
      instance.run();
    }
    final Schema schema = instance.getAvroSchema();
    if (schema != null && !schemaFilePath.isEmpty()) {
      try (OutputStream out = new BufferedOutputStream(new FileOutputStream(schemaFilePath))) {
        out.write(schema.toString(true).getBytes("UTF-8"));
        out.flush();
      }
    }
  }

  public Schema getAvroSchema() {
    return avroSchema;
  }

  public void validate() {
    if (out == null) {
      throw new IllegalStateException("Output stream must be valid.");
    }
    switch (inFormat) {
      case JDBC:
        if (jdbcConnection == null || jdbcQuery == null) {
          throw new IllegalStateException("JDBC connection and query must be valid.");
        }
        return;
      case AVRO_BINARY:
      case AVRO_JSON:
        if (declaredAvroSchema == null) {
          throw new IllegalStateException("Avro schema was not provided.");
        }
        break;
      case THRIFT_BINARY:
      case THRIFT_COMPACT:
      case THRIFT_JSON:
        if (!TBase.class.isAssignableFrom(thriftClass)) {
          throw new IllegalStateException("Thrift class was not provided.");
        }
        break;
    }
    if (in == null) {
      throw new IllegalStateException("Input stream must be valid.");
    }
  }

  public void runAvro(Stream<GenericRecord> stream) {
    switch (outFormat.category) {
      case AVRO:
        final Supplier<WriteConsumer<GenericRecord>> avroSupplier;
        switch (outFormat) {
          case AVRO_BINARY:
            avroSupplier = () -> AvroConsumers.binary(avroSchema, out);
            break;
          case AVRO_JSON:
            avroSupplier = () -> AvroConsumers.json(avroSchema, out);
            break;
          default:
            avroSupplier = () -> AvroConsumers.batch(avroSchema, out);
            break;
        }
        try (WriteConsumer<GenericRecord> writeConsumer = avroSupplier.get()) {
          stream.sequential().forEach(writeConsumer);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      case JACKSON:
        final JacksonConsumers<? extends TreeNode, ?, ?, ?, ?> jacksonConsumers;
        final boolean strict;
        switch (outFormat) {
          case CBOR:
            jacksonConsumers = new CborConsumers();
            break;
          case CSV:
          case CSV_NO_HEADER:
          case TSV:
          case TSV_NO_HEADER:
            CsvSchema.Builder builder = CsvSchema.builder();
            builder.setUseHeader(outFormat == Format.CSV || outFormat == Format.TSV);
            if (outFormat == Format.TSV || outFormat == Format.TSV_NO_HEADER) {
              builder.setColumnSeparator('\t');
            }
            avroSchema.getFields().stream()
                .map(Schema.Field::name)
                .forEach(builder::addColumn);
            jacksonConsumers = new CsvConsumers(builder.build());
            break;
          case JSON:
            jacksonConsumers = new JsonConsumers();
            break;
          case SMILE:
            jacksonConsumers = new SmileConsumers();
            break;
          default:
            throw new IllegalArgumentException("Unsupported output format " + outFormat);
        }
        try (JsonGenerator generator = jacksonConsumers.generator(out)) {
          try (WriteConsumer<GenericRecord> consumer =
                   AvroConsumers.jackson(avroSchema, generator, outFormat.writeStrict)) {
            stream.sequential().forEach(consumer);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      default:
        throw new IllegalArgumentException("Unsupported output format " + outFormat);
    }
  }

  public void runProtobuf(Stream<M> stream) {
    switch (outFormat.category) {
      case PROTOBUF:
        try (WriteConsumer<M> writeConsumer = ProtobufConsumers.binary(out)) {
          stream.sequential().forEach(writeConsumer);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      case JACKSON:
        final JacksonConsumers<? extends TreeNode, ?, ?, ?, ?> jacksonConsumers;
        switch (outFormat) {
          case CBOR:
            jacksonConsumers = new CborConsumers();
            break;
          case CSV:
          case CSV_NO_HEADER:
          case TSV:
          case TSV_NO_HEADER:
            CsvSchema.Builder builder = CsvSchema.builder();
            builder.setUseHeader(outFormat == Format.CSV || outFormat == Format.TSV);
            if (outFormat == Format.TSV || outFormat == Format.TSV_NO_HEADER) {
              builder.setColumnSeparator('\t');
            }
            AnoaReflectionUtils.getProtobufDescriptor(protobufClass).getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .forEach(builder::addColumn);
            jacksonConsumers = new CsvConsumers(builder.build());
            break;
          case JSON:
            jacksonConsumers = new JsonConsumers();
            break;
          case SMILE:
            jacksonConsumers = new SmileConsumers();
            break;
          default:
            throw new IllegalArgumentException("Unsupported output format " + outFormat);
        }
        try (JsonGenerator generator = jacksonConsumers.generator(out)) {
          try (WriteConsumer<M> consumer =
                   ProtobufConsumers.jackson(protobufClass, generator, outFormat.writeStrict)) {
            stream.sequential().forEach(consumer);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      default:
        throw new IllegalArgumentException("Unsupported output format " + outFormat);
    }
  }

  public void runThrift(Stream<T> stream) {
    switch (outFormat.category) {
      case THRIFT:
        final Supplier<WriteConsumer<T>> thriftSupplier;
        switch (outFormat) {
          case THRIFT_COMPACT:
            thriftSupplier = () -> ThriftConsumers.compact(out);
            break;
          case THRIFT_JSON:
            thriftSupplier = () -> ThriftConsumers.json(out);
            break;
          default:
            thriftSupplier = () -> ThriftConsumers.binary(out);
        }
        try (WriteConsumer<T> writeConsumer = thriftSupplier.get()) {
          stream.sequential().forEach(writeConsumer);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      case JACKSON:
        final JacksonConsumers<? extends TreeNode, ?, ?, ?, ?> jacksonConsumers;
        switch (outFormat) {
          case CBOR:
            jacksonConsumers = new CborConsumers();
            break;
          case CSV:
          case CSV_NO_HEADER:
          case TSV:
          case TSV_NO_HEADER:
            CsvSchema.Builder builder = CsvSchema.builder();
            builder.setUseHeader(outFormat == Format.CSV || outFormat == Format.TSV);
            if (outFormat == Format.TSV || outFormat == Format.TSV_NO_HEADER) {
              builder.setColumnSeparator('\t');
            }
            AnoaReflectionUtils.getThriftMetaDataMap(thriftClass).keySet().stream()
                .map(TFieldIdEnum::getFieldName)
                .forEach(builder::addColumn);
            jacksonConsumers = new CsvConsumers(builder.build());
            break;
          case JSON:
            jacksonConsumers = new JsonConsumers();
            break;
          case SMILE:
            jacksonConsumers = new SmileConsumers();
            break;
          default:
            throw new IllegalArgumentException("Unsupported output format " + outFormat);
        }
        try (JsonGenerator generator = jacksonConsumers.generator(out)) {
          try (WriteConsumer<T> consumer =
                   ThriftConsumers.jackson(thriftClass, generator, outFormat.writeStrict)) {
            stream.sequential().forEach(consumer);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      default:
        throw new IllegalArgumentException("Unsupported output format " + outFormat);
    }
  }

  public void runJackson(Stream<ObjectNode> stream) {
    if (avroSchema != null) {
      runAvro(stream.map(TreeNode::traverse)
                  .map(AvroDecoders.jackson(avroSchema, false)));
      return;
    } else if (thriftClass != null) {
      runThrift(stream.map(TreeNode::traverse)
                    .map(ThriftDecoders.jackson(thriftClass, false)));
      return;
    } else if (protobufClass != null) {
      runProtobuf(stream.map(TreeNode::traverse)
                      .map(ProtobufDecoders.jackson(protobufClass, false)));
    }
    final Supplier<JacksonConsumers<ObjectNode, ?, ?, ?, ?>> supplier;
    switch (outFormat) {
      case CBOR:
        supplier = CborConsumers::new;
        break;
      case JSON:
        supplier = JsonConsumers::new;
        break;
      case SMILE:
        supplier = SmileConsumers::new;
        break;
      default:
        throw new IllegalArgumentException("Unsupported output format " + outFormat);
    }
    try (WriteConsumer<ObjectNode> writeConsumer = supplier.get().to(out)) {
      stream.sequential().forEach(writeConsumer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void run() {
    validate();
    switch (inFormat) {
      case AVRO:
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(declaredAvroSchema);
        try (DataFileStream<GenericRecord> it = new DataFileStream<>(in, reader)) {
          avroSchema = reader.getExpected();
          runAvro(AvroStreams.batch(it));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      case AVRO_BINARY:
        runAvro(AvroStreams.binary(declaredAvroSchema, in));
        return;
      case AVRO_JSON:
        runAvro(AvroStreams.json(declaredAvroSchema, in));
        return;
      case CBOR:
        runJackson(new CborStreams().from(in));
        return;
      case CSV:
        runJackson(CsvStreams.csvWithHeader().from(in));
        return;
      case CSV_NO_HEADER:
        runJackson(CsvStreams.csv().from(in));
        return;
      case THRIFT_BINARY:
        runThrift(ThriftStreams.binary(Unchecked.supplier(thriftClass::newInstance), in));
        return;
      case THRIFT_COMPACT:
        runThrift(ThriftStreams.compact(Unchecked.supplier(thriftClass::newInstance), in));
        return;
      case THRIFT_JSON:
        runThrift(ThriftStreams.json(Unchecked.supplier(thriftClass::newInstance), in));
        return;
      case PROTOBUF:
        runProtobuf(ProtobufStreams.binary(protobufClass, false, in));
        return;
      case JDBC:
        try (Statement statement = jdbcConnection.createStatement()) {
          statement.setFetchSize(jdbcFetchSize);
          for (String initStatement : jdbcInitStatements) {
            statement.execute(initStatement);
          }
          final ResultSet resultSet = statement.executeQuery(jdbcQuery);
          if (avroSchema == null) {
            avroSchema = JdbcStreams.induceSchema(resultSet.getMetaData());
          }
          runAvro(new JdbcStreams().resultSet(resultSet)
                      .map(ObjectNode::traverse)
                      .map(AvroDecoders.jackson(avroSchema, false)));
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return;
      case JSON:
        runJackson(new JsonStreams().from(in));
        return;
      case SMILE:
        runJackson(new SmileStreams().from(in));
        return;
      case TSV:
        runJackson(CsvStreams.tsvWithHeader().from(in));
        return;
      case TSV_NO_HEADER:
        runJackson(CsvStreams.tsv().from(in));
        return;
    }
    throw new UnsupportedOperationException();
  }
}
