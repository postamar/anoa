package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.factory.AvroConsumers;
import com.adgear.anoa.factory.AvroGenericStreams;
import com.adgear.anoa.factory.CborObjects;
import com.adgear.anoa.factory.CsvObjects;
import com.adgear.anoa.factory.JacksonObjects;
import com.adgear.anoa.factory.JdbcStreams;
import com.adgear.anoa.factory.JsonObjects;
import com.adgear.anoa.factory.SmileObjects;
import com.adgear.anoa.factory.ThriftConsumers;
import com.adgear.anoa.factory.ThriftStreams;
import com.adgear.anoa.factory.XmlObjects;
import com.adgear.anoa.factory.YamlObjects;
import com.adgear.anoa.factory.util.ReflectionUtils;
import com.adgear.anoa.factory.util.WriteConsumer;
import com.adgear.anoa.read.AnoaRead;
import com.adgear.anoa.tools.data.Format;
import com.adgear.anoa.write.AnoaWrite;
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
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataTool<F extends TFieldIdEnum, T extends TBase<T,F>> implements Runnable {

  final private Schema declaredAvroSchema;
  final private Class<T> thriftClass;
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
   * Constructor for when reading from JDBC source.
   *
   * @param avroSchema         Declared Avro record schema (optional).
   * @param outFormat          Declared output serialization format.
   * @param out                Stream to write records to.
   * @param jdbcConnection     Connection object used to open session.
   * @param jdbcInitStatements Array of individual statements to be executed before query.
   * @param jdbcQuery          SQL query to submit.
   * @param jdbcFetchSize      JDBC result set fetch size.
   */
  public DataTool(Schema avroSchema,
                  Class<T> thriftClass,
                  Format outFormat,
                  OutputStream out,
                  Connection jdbcConnection,
                  List<String> jdbcInitStatements,
                  String jdbcQuery,
                  int jdbcFetchSize) throws Exception {
    this.avroSchema = this.declaredAvroSchema = avroSchema;
    this.thriftClass = thriftClass;
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
   * Constructor for when reading from stream.
   *
   * @param avroSchema  Declared Avro record schema (optional for some input formats).
   * @param inFormat    Declared input serialization format.
   * @param outFormat   Declared output serialization format.
   * @param in          Stream to read records from.
   * @param out         Stream to write records to.
   */
  public DataTool(Schema avroSchema,
                  Class<T> thriftClass,
                  Format inFormat,
                  Format outFormat,
                  InputStream in,
                  OutputStream out) {
    this.avroSchema = this.declaredAvroSchema = avroSchema;
    this.thriftClass = thriftClass;
    this.inFormat = inFormat;
    this.outFormat = outFormat;
    this.in = in;
    this.out = out;
    this.jdbcConnection = null;
    this.jdbcInitStatements = null;
    this.jdbcQuery = null;
    this.jdbcFetchSize = -1;
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
            avroSupplier = () -> AvroConsumers.binary(out, avroSchema);
            break;
          case AVRO_JSON:
            avroSupplier = () -> AvroConsumers.json(out, avroSchema);
            break;
          default:
            avroSupplier = () -> AvroConsumers.batch(out, avroSchema);
            break;
        }
        try (WriteConsumer<GenericRecord> writeConsumer = avroSupplier.get()) {
          stream.forEach(writeConsumer);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      case JACKSON:
        final JacksonObjects<?,?,?,?,?> jacksonObjects;
        switch (outFormat) {
          case CBOR:
            jacksonObjects = new CborObjects();
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
            jacksonObjects = new CsvObjects(builder.build());
            break;
          case JSON:
            jacksonObjects = new JsonObjects();
            break;
          case SMILE:
            jacksonObjects = new SmileObjects();
            break;
          case XML:
            jacksonObjects = new XmlObjects();
            break;
          case YAML:
            jacksonObjects = new YamlObjects();
            break;
          default:
            throw new IllegalArgumentException("Unsupported output format " + outFormat);
        }
        BiConsumer<GenericRecord, JsonGenerator> biConsumer = AnoaWrite.biCo(avroSchema);
        try (JsonGenerator generator = jacksonObjects.generator(out)) {
          stream.sequential().forEach(record -> biConsumer.accept(record, generator));
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
          stream.forEach(writeConsumer);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      case JACKSON:
        final JacksonObjects<?,?,?,?,?> jacksonObjects;
        switch (outFormat) {
          case CBOR:
            jacksonObjects = new CborObjects();
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
            ReflectionUtils.getThriftMetaDataMap(thriftClass).keySet().stream()
                .map(TFieldIdEnum::getFieldName)
                .forEach(builder::addColumn);
            jacksonObjects = new CsvObjects(builder.build());
            break;
          case JSON:
            jacksonObjects = new JsonObjects();
            break;
          case SMILE:
            jacksonObjects = new SmileObjects();
            break;
          case XML:
            jacksonObjects = new XmlObjects();
            break;
          case YAML:
            jacksonObjects = new YamlObjects();
            break;
          default:
            throw new IllegalArgumentException("Unsupported output format " + outFormat);
        }
        BiConsumer<T, JsonGenerator> biConsumer = AnoaWrite.biCo(thriftClass);
        try (JsonGenerator generator = jacksonObjects.generator(out)) {
          stream.sequential().forEach(record -> biConsumer.accept(record, generator));
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
      runAvro(stream.map(TreeNode::traverse).map(AnoaRead.fn(avroSchema, false)));
      return;
    } else if (thriftClass != null) {
      runThrift(stream.map(TreeNode::traverse).map(AnoaRead.fn(thriftClass, false)));
      return;
    }
    final Supplier<JacksonObjects<?,?,?,?,?>> supplier;
    switch (outFormat) {
      case CBOR:
        supplier = CborObjects::new;
        break;
      case JSON:
        supplier = JsonObjects::new;
        break;
      case SMILE:
        supplier = SmileObjects::new;
        break;
      case XML:
        supplier = XmlObjects::new;
        break;
      case YAML:
        supplier = YamlObjects::new;
        break;
      default:
        throw new IllegalArgumentException("Unsupported output format " + outFormat);
    }
    try (WriteConsumer<ObjectNode> writeConsumer = supplier.get().to(out)) {
      stream.forEach(writeConsumer);
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
          avroSchema = reader.getSchema();
          runAvro(AvroGenericStreams.from(it));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        return;
      case AVRO_BINARY:
        runAvro(AvroGenericStreams.binary(in, declaredAvroSchema));
        return;
      case AVRO_JSON:
        runAvro(AvroGenericStreams.json(in, declaredAvroSchema));
        return;
      case CBOR:
        runJackson(new CborObjects().from(in));
        return;
      case CSV:
        runJackson(CsvObjects.csvWithHeader().from(in));
        return;
      case CSV_NO_HEADER:
        runJackson(CsvObjects.csv().from(in));
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
      case JDBC:
        try (Statement statement = jdbcConnection.createStatement()) {
          statement.setFetchSize(jdbcFetchSize);
          for (String initStatement : jdbcInitStatements) {
            statement.execute(initStatement);
          }
          try (ResultSet resultSet = statement.executeQuery(jdbcQuery)) {
            if (avroSchema == null) {
              avroSchema = JdbcStreams.induceSchema(resultSet.getMetaData());
            }
            runAvro(JdbcStreams.from(resultSet)
                        .map(AnoaRead.fn(avroSchema, false).compose(TreeNode::traverse)));
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return;
      case JSON:
        runJackson(new JsonObjects().from(in));
        return;
      case SMILE:
        runJackson(new SmileObjects().from(in));
        return;
      case TSV:
        runJackson(CsvObjects.tsvWithHeader().from(in));
        return;
      case TSV_NO_HEADER:
        runJackson(CsvObjects.tsv().from(in));
        return;
      case XML:
        runJackson(new XmlObjects().from(in));
        return;
      case YAML:
        runJackson(new YamlObjects().from(in));
        return;
    }
    throw new UnsupportedOperationException();
  }

  static public void main(String[] args) throws Exception {
    final Format inFormat = Format.valueOfIgnoreCase(System.getProperty("inFormat", ""));
    final Format outFormat = Format.valueOfIgnoreCase(System.getProperty("outFormat", ""));
    final String avroClassName = System.getProperty("recordClass", "");
    final String thriftClassName = System.getProperty("thriftClass", "");
    final String outFilePath = System.getProperty("out", "");
    final String inFilePath = System.getProperty("in", "");

    Schema avroSchema = null;
    if (!avroClassName.isEmpty()) {
      Class<? extends SpecificRecord> avroRecordClass = ReflectionUtils.getAvroClass(avroClassName);
      if (avroRecordClass != null) {
        avroSchema = SpecificData.get().getSchema(avroRecordClass);
      }
    }
    Class<? extends TBase> thriftRecordClass = null;
    if (!thriftClassName.isEmpty()) {
      thriftRecordClass = ReflectionUtils.getThriftClass(thriftClassName);
    }

    final OutputStream out = outFilePath.isEmpty() ? System.out : new FileOutputStream(outFilePath);
    final DataTool instance;

    if (inFormat == Format.JDBC) {
      final String jdbcUrl = System.getProperty("url");
      final String jdbcQuery = System.getProperty("query");
      final String initPath = System.getProperty("initScript", "");
      Stream<String> initStream = initPath.isEmpty()
                                  ? Stream.<String>empty()
                                  : new BufferedReader(new FileReader(initPath)).lines();
      instance = new DataTool<>(avroSchema,
                                thriftRecordClass,
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
                                inFormat,
                                outFormat,
                                in,
                                out);
    }
    instance.run();
  }
}
