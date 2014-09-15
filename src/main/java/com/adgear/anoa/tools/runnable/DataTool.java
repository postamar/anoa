package com.adgear.anoa.tools.runnable;

import com.adgear.anoa.codec.avro.JsonNodeToAvro;
import com.adgear.anoa.codec.avro.StringListToAvro;
import com.adgear.anoa.codec.avro.ValueToAvro;
import com.adgear.anoa.codec.schemaless.AvroGenericToStringList;
import com.adgear.anoa.codec.schemaless.AvroGenericToValue;
import com.adgear.anoa.codec.serialized.ValueToBytes;
import com.adgear.anoa.codec.serialized.ValueToJsonBytes;
import com.adgear.anoa.provider.Provider;
import com.adgear.anoa.provider.avro.AvroProvider;
import com.adgear.anoa.sink.Sink;
import com.adgear.anoa.sink.avro.AvroSink;
import com.adgear.anoa.sink.schemaless.CsvSink;
import com.adgear.anoa.sink.schemaless.TsvSink;
import com.adgear.anoa.sink.serialized.BytesLineSink;
import com.adgear.anoa.sink.serialized.BytesSink;
import com.adgear.anoa.source.Source;
import com.adgear.anoa.source.avro.AvroGenericSource;
import com.adgear.anoa.source.avro.AvroSource;
import com.adgear.anoa.source.schemaless.CsvSource;
import com.adgear.anoa.source.schemaless.CsvWithHeaderSource;
import com.adgear.anoa.source.schemaless.JdbcSource;
import com.adgear.anoa.source.schemaless.JsonNodeSource;
import com.adgear.anoa.source.schemaless.TsvSource;
import com.adgear.anoa.source.schemaless.TsvWithHeaderSource;
import com.adgear.anoa.source.schemaless.ValueSource;
import com.adgear.anoa.source.serialized.StringLineSource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A tool for converting between various serialization formats.
 *
 * @param <R> Type of the records to be processed, must extend Avro's {@link
 *            org.apache.avro.specific.SpecificRecord}.
 */
public class DataTool<R extends SpecificRecord> extends ToolBase {

  final private Class<R> recordClass;
  final private Format inFormat;
  final private Format outFormat;
  final private InputStream in;
  final private OutputStream out;
  final private OutputStream schemaOut;
  final private Connection jdbcConnection;
  final private String[] jdbcInitStatements;
  final private String jdbcQuery;
  final private int jdbcFetchSize;
  private Source<?> source;
  private Sink<?, ?> sink;

  /**
   * Constructor for when reading from JDBC source.
   *
   * @param recordClass        Declared record class (optional).
   * @param outFormat          Declared output serialization format.
   * @param out                Stream to write records to.
   * @param schemaOut          Stream to write Avro schema to (optional).
   * @param jdbcConnection     Connection object used to open session.
   * @param jdbcInitStatements Array of individual SQL statements to be executed before submitting
   *                           query.
   * @param jdbcQuery          SQL query to submit.
   * @param jdbcFetchSize      JDBC result set fetch size.
   */
  public DataTool(Class<R> recordClass,
                  Format outFormat,
                  OutputStream out,
                  OutputStream schemaOut,
                  Connection jdbcConnection,
                  String[] jdbcInitStatements,
                  String jdbcQuery,
                  int jdbcFetchSize) {
    this.recordClass = recordClass;
    this.inFormat = Format.JDBC;
    this.outFormat = outFormat;
    this.in = null;
    this.out = out;
    this.schemaOut = schemaOut;
    this.jdbcConnection = jdbcConnection;
    this.jdbcInitStatements = jdbcInitStatements;
    this.jdbcQuery = jdbcQuery;
    this.jdbcFetchSize = jdbcFetchSize;
  }

  /**
   * Constructor for when reading from stream.
   *
   * @param recordClass Declared record class (optional for some input formats).
   * @param inFormat    Declared input serialization format.
   * @param outFormat   Declared output serialization format.
   * @param in          Stream to read records from.
   * @param out         Stream to write records to.
   * @param schemaOut   Stream to output Avro schema (optional).
   */
  public DataTool(Class<R> recordClass,
                  Format inFormat,
                  Format outFormat,
                  InputStream in,
                  OutputStream out,
                  OutputStream schemaOut) {
    this.recordClass = recordClass;
    this.inFormat = inFormat;
    this.outFormat = outFormat;
    this.in = in;
    this.out = out;
    this.schemaOut = schemaOut;
    this.jdbcConnection = null;
    this.jdbcInitStatements = null;
    this.jdbcQuery = null;
    this.jdbcFetchSize = -1;
  }

  static private Format getFormat(String fmt) {
    for (Format value : Format.values()) {
      if (value.toString().equals(fmt.toUpperCase())) {
        return value;
      }
    }
    throw new UnsupportedOperationException("Unknown format '" + fmt + "'.");
  }

  /**
   * Builds object and calls {@link #execute()}.
   *
   * @param jdbcUrl              JDBC URL to data source.
   * @param jdbcInitScriptStream Stream to SQL session initialization statements, separated by
   *                             newlines.
   * @param jdbcQuery            SQL query to submit.
   * @param jdbcProperties       Properties object for parameterizing the JDBC connection.
   */
  static public <R extends SpecificRecord> void jdbcRun(Class<R> recordClass,
                                                        Format outFormat,
                                                        OutputStream out,
                                                        OutputStream schemaOut,
                                                        String jdbcUrl,
                                                        InputStream jdbcInitScriptStream,
                                                        String jdbcQuery,
                                                        Properties jdbcProperties)
      throws IOException, SQLException {
    Connection jdbcConnection = DriverManager.getConnection(jdbcUrl,
                                                            jdbcProperties);

    ArrayList<String> initStatementList = new ArrayList<>();
    if (jdbcInitScriptStream != null) {
      for (String statement : new StringLineSource(new InputStreamReader(jdbcInitScriptStream))) {
        initStatementList.add(statement);
      }
    }
    String[] jdbcInitStatements = initStatementList.toArray(new String[initStatementList.size()]);
    new DataTool<>(recordClass,
                   outFormat,
                   out,
                   schemaOut,
                   jdbcConnection,
                   jdbcInitStatements,
                   jdbcQuery,
                   4096)
        .execute();
  }

  /**
   * Builds object and calls {@link #execute()}.
   */
  static public <R extends SpecificRecord> void streamRun(Class<R> recordClass,
                                                          Format inFormat,
                                                          Format outFormat,
                                                          InputStream in,
                                                          OutputStream out,
                                                          OutputStream schemaOut)
      throws IOException {
    new DataTool<>(recordClass,
                   inFormat,
                   outFormat,
                   in,
                   out,
                   schemaOut)
        .execute();
  }

  static public <R extends SpecificRecord> void main(String[] args)
      throws IOException, SQLException, ClassNotFoundException {

    Format inFormat = getFormat(System.getProperty("inFormat", ""));
    Format outFormat = getFormat(System.getProperty("outFormat", ""));
    String className = System.getProperty("recordClass", "");
    String schemaFilePath = System.getProperty("schemaFilePath", "");
    String outFilePath = System.getProperty("out", "");
    String inFilePath = System.getProperty("in", "");

    Class<R> recordClass = null;
    if (className.length() > 0) {
      recordClass = getRecordClass(className);
    }

    OutputStream schemaOut = null;
    if (schemaFilePath.length() > 0) {
      schemaOut = new FileOutputStream(schemaFilePath);
    }

    OutputStream out = System.out;
    if (outFilePath.length() > 0) {
      out = new FileOutputStream(outFilePath);
    }

    if (inFormat == Format.JDBC) {
      String jdbcUrl = System.getProperty("url");
      String jdbcQuery = System.getProperty("query");
      String initScriptPath = System.getProperty("initScript", "");
      InputStream jdbcInitScriptStream = null;
      if (initScriptPath.length() > 0) {
        jdbcInitScriptStream = new FileInputStream(initScriptPath);
      }
      jdbcRun(recordClass,
              outFormat,
              out,
              schemaOut,
              jdbcUrl,
              jdbcInitScriptStream,
              jdbcQuery,
              System.getProperties());
      return;
    }

    InputStream in = System.in;
    if (inFilePath.length() > 0) {
      in = new FileInputStream(inFilePath);
    }

    streamRun(recordClass, inFormat, outFormat, in, out, schemaOut);
  }

  /**
   * Iterates through the data source, deserializes each record, serializes it to the output
   * stream.
   */
  @Override
  public void execute() throws IOException {
    source = null;
    sink = null;
    try {
      AvroProvider<GenericRecord> avroProvider = (recordClass == null)
                                                 ? createInducedSource()
                                                 : createSpecifiedSource();
      if (schemaOut != null) {
        schemaOut.write(avroProvider.getAvroSchema().toString(true).getBytes("UTF-8"));
        schemaOut.flush();
      }
      Provider<?> last = writeToSink(avroProvider);
      if (last.getCountDropped() > 0) {
        Logger logger = LoggerFactory.getLogger(this.getClass());
        logger.warn(String.format("Dropped %d out of %d records.",
                                  last.getCountDropped(),
                                  last.getCountTotal()));
      }
    } finally {
      closeAll();
    }
  }

  private AvroProvider<GenericRecord> createInducedSource() throws IOException {
    if (inFormat == Format.AVRO) {
      AvroGenericSource avroSource = new AvroGenericSource(in);
      source = avroSource;
      return avroSource;
    }
    final AvroSource<List<String>> stringListSource;
    switch (inFormat) {
      case JDBC:
        try {
          stringListSource = new JdbcSource(fetchResultSet());
        } catch (SQLException e) {
          throw new IOException(e);
        }
        break;
      case CSV:
        stringListSource = new CsvWithHeaderSource(new InputStreamReader(in));
        break;
      case TSV:
        stringListSource = new TsvWithHeaderSource(new InputStreamReader(in));
        break;
      default:
        throw new UnsupportedOperationException("Unsupported input format " + inFormat);
    }
    source = stringListSource;
    return new StringListToAvro<>(stringListSource, stringListSource.getAvroSchema());
  }

  private AvroProvider<GenericRecord> createSpecifiedSource() throws IOException {
    final Schema schema = SpecificData.get().getSchema(recordClass);
    switch (inFormat) {
      case AVRO:
        return new AvroGenericSource(in, schema);
      case MSGPACK:
        return new ValueToAvro<>(new ValueSource(in), schema);
      case JSON:
        return new JsonNodeToAvro<>(new JsonNodeSource(in), schema);
      case CSV:
        return new StringListToAvro<>(new CsvWithHeaderSource(new InputStreamReader(in)), schema);
      case CSV_NO_HEADER:
        return new StringListToAvro<>(new CsvSource(new InputStreamReader(in)), schema);
      case TSV:
        return new StringListToAvro<>(new TsvWithHeaderSource(new InputStreamReader(in)), schema);
      case TSV_NO_HEADER:
        return new StringListToAvro<>(new TsvSource(new InputStreamReader(in)), schema);
      case JDBC:
        try {
          return new StringListToAvro<>(new JdbcSource(fetchResultSet()), schema);
        } catch (SQLException e) {
          throw new IOException(e);
        }
    }
    throw new UnsupportedOperationException("Unsupported input format " + inFormat);
  }

  private Provider<?> writeToSink(AvroProvider<GenericRecord> avroProvider) throws IOException {
    final Schema schema = avroProvider.getAvroSchema();
    switch (outFormat) {
      case AVRO:
        sink = new AvroSink<GenericRecord>(out, schema).appendAll(avroProvider);
        return avroProvider;
      case MSGPACK: {
        Provider<byte[]> codec = new ValueToBytes(new AvroGenericToValue(avroProvider));
        sink = new BytesSink(out).appendAll(codec);
        return codec;
      }
      case JSON: {
        Provider<byte[]> codec = new ValueToJsonBytes(new AvroGenericToValue(avroProvider));
        sink = new BytesLineSink(out).appendAll(codec);
        return codec;
      }
      case CSV: {
        Provider<List<String>> codec = new AvroGenericToStringList(avroProvider);
        sink = new CsvSink(out, schema).appendAll(codec);
        return codec;
      }
      case CSV_NO_HEADER: {
        Provider<List<String>> codec = new AvroGenericToStringList(avroProvider);
        sink = new CsvSink(out).appendAll(codec);
        return codec;
      }
      case TSV: {
        Provider<List<String>> codec = new AvroGenericToStringList(avroProvider);
        sink = new TsvSink(out, schema).appendAll(codec);
        return codec;
      }
      case TSV_NO_HEADER: {
        Provider<List<String>> codec = new AvroGenericToStringList(avroProvider);
        sink = new TsvSink(out).appendAll(codec);
        return codec;
      }
      default:
        throw new UnsupportedOperationException("Unsupported output format " + outFormat);
    }
  }

  private ResultSet fetchResultSet() throws SQLException {
    Statement statement = jdbcConnection.createStatement();
    statement.setFetchSize(jdbcFetchSize);
    for (String initStatement : jdbcInitStatements) {
      statement.execute(initStatement);
    }
    return statement.executeQuery(jdbcQuery);
  }

  private void closeAll() throws IOException {
    if (sink != null) {
      sink.close();
    }
    if (source != null) {
      source.close();
    }
    if (schemaOut != null) {
      schemaOut.close();
    }
    if (jdbcConnection != null) {
      try {
        jdbcConnection.close();
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Supported input/output serialization formats.
   */
  static public enum Format {
    /**
     * Avro batch file
     */
    AVRO,

    /**
     * JDBC result set (input only)
     */
    JDBC,

    /**
     * MessagePack objects
     */
    MSGPACK,

    /**
     * JSON objects
     */
    JSON,

    /**
     * CSV with column header row, separated by ',', escaping supported, no trimming.
     */
    CSV,

    /**
     * same as {@link #CSV}, but without column header row.
     */
    CSV_NO_HEADER,

    /**
     * TSV with column header row, separated by ',', escaping supported, no trimming.
     */
    TSV,

    /**
     * same as {@link #TSV}, but without column header row.
     */
    TSV_NO_HEADER
  }
}