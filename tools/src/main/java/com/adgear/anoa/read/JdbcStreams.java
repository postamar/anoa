package com.adgear.anoa.read;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaFactory;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.Schema;
import org.apache.commons.codec.digest.DigestUtils;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.jooq.lambda.SQL;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedFunction;
import org.jooq.lambda.tuple.Tuple;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utility class for streaming Jackson records from a JDBC result set.
 */
public class JdbcStreams {

  /**
   * Create with default object mapper
   */
  public JdbcStreams() {
    this(new ObjectMapper());
  }

  /**
   * @param objectCodec Object mapper to use
   */
  public JdbcStreams(ObjectCodec objectCodec) {
    this.objectCodec = objectCodec;
  }

  /**
   * Object mapper used by this instance
   */
  final public ObjectCodec objectCodec;

  /**
   * @param resultSet the JDBC result set to scan
   * @return A stream of Jackson records which map to the result set rows.
   */
  public @NonNull Stream<ObjectNode> resultSet(@NonNull ResultSet resultSet) {
    final Function<ResultSet, ObjectNode> fn;
    try {
      fn = Unchecked.function(rowFn(resultSet.getMetaData()));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return SQL.seq(resultSet, fn);
  }

  /**
   *
   * @param anoaFactory {@code AnoaFactory} instance to use for exception handling
   * @param resultSet the JDBC result set to scan
   * @param <M> Metadata type
   * @return A stream of Jackson records which map to the result set rows.
   */
  public <M> @NonNull Stream<Anoa<ObjectNode, M>> resultSet(
      @NonNull AnoaFactory<M> anoaFactory,
      @NonNull ResultSet resultSet) {
    final CheckedFunction<ResultSet, ObjectNode> fn;
    try {
      fn = rowFn(resultSet.getMetaData());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return SQL.seq(resultSet, (ResultSet rs) -> {
      final ObjectNode objectNode;
      try {
        objectNode = fn.apply(rs);
      } catch (Throwable throwable) {
        return new Anoa<>(anoaFactory.handler.apply(throwable, Tuple.tuple(rs)));
      }
      return anoaFactory.wrap(objectNode);
    });
  }

  protected CheckedFunction<ResultSet, ObjectNode> rowFn(
      ResultSetMetaData rsmd) throws SQLException {
    int n = rsmd.getColumnCount();
    String[] names = new String[n];
    for (int c = 0; c < n; c++) {
      names[c] = rsmd.getColumnLabel(c + 1);
    }
    return (ResultSet resultSet) -> {
      TokenBuffer tb = new TokenBuffer(objectCodec, false);
      tb.writeStartObject();
      for (int c = 0; c < n;) {
        tb.writeFieldName(names[c++]);
        tb.writeObject(resultSet.getObject(c));
      }
      tb.writeEndObject();
      return tb.asParser(objectCodec).readValueAsTree();
    };
  }

  /**
   * @param rsmd JDBC Result Set metadata
   * @return equivalent Avro Schema
   * @throws SQLException
   */
  static public @NonNull Schema induceSchema(@NonNull ResultSetMetaData rsmd) throws SQLException {
    List<Schema.Field> fields = IntStream.range(1, rsmd.getColumnCount() + 1)
        .mapToObj(Unchecked.intFunction(c -> {
          String label = rsmd.getColumnLabel(c);
          Schema.Field f = new Schema.Field(
              label.toLowerCase(),
              Schema.createUnion(Stream.of(Schema.Type.NULL, getAvroType(rsmd.getColumnType(c)))
                                     .map(Schema::create)
                                     .collect(Collectors.toList())),
              null,
              JsonNodeFactory.instance.nullNode());
          if (!f.name().equals(label)) {
            f.addAlias(label);
          }
          return f;
        }))
        .collect(Collectors.toList());
    Schema avroSchema = Schema.createRecord("jdbc_" + DigestUtils.sha1Hex(fields.toString()),
                                            "induced from JDBC ResultSetMetaData",
                                            "com.adgear.avro.induced",
                                            false);
    avroSchema.setFields(fields);
    return avroSchema;
  }

  static protected Schema.Type getAvroType(int sqlType) {
    switch (sqlType) {
      case Types.BIT:
      case Types.BOOLEAN:
        return Schema.Type.BOOLEAN;
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        return Schema.Type.INT;
      case Types.BIGINT:
        return Schema.Type.LONG;
      case Types.DECIMAL:
      case Types.NUMERIC:
      case Types.REAL:
        return Schema.Type.STRING;
      case Types.FLOAT:
        return Schema.Type.FLOAT;
      case Types.DOUBLE:
        return Schema.Type.DOUBLE;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return Schema.Type.BYTES;
      case Types.NULL:
        return Schema.Type.NULL;
      default:
        return Schema.Type.STRING;
    }
  }
}