package com.adgear.anoa.read;

import com.adgear.anoa.Anoa;
import com.adgear.anoa.AnoaHandler;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.Schema;
import org.apache.commons.codec.digest.DigestUtils;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.jooq.lambda.SQL;
import org.jooq.lambda.Unchecked;
import org.jooq.lambda.fi.util.function.CheckedFunction;

import java.io.IOException;
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
   * Object mapper used by this instance
   */
  final public ObjectCodec objectCodec;

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
   * @param rsmd JDBC Result Set metadata
   * @return equivalent Avro Schema
   */
  static public Schema induceSchema(ResultSetMetaData rsmd) throws SQLException {
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
    Schema avroSchema = Schema.createRecord("jdbc_" + DigestUtils.md5Hex(fields.toString()),
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

  /**
   * @param resultSet the JDBC result set to scan
   * @return A stream of Jackson records which map to the result set rows.
   */
  public Stream<ObjectNode> resultSet(ResultSet resultSet) {
    final Function<ResultSet, ObjectNode> fn;
    try {
      fn = Unchecked.function(rowFn(resultSet.getMetaData()));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return SQL.seq(resultSet, fn);
  }

  /**
   * @param anoaHandler {@code AnoaHandler} instance to use for exception handling
   * @param resultSet   the JDBC result set to scan
   * @param <M>         Metadata type
   * @return A stream of Jackson records which map to the result set rows.
   */
  public <M> Stream<Anoa<ObjectNode, M>> resultSet(
      AnoaHandler<M> anoaHandler,
      ResultSet resultSet) {
    final Function<Anoa<ResultSet, M>, Anoa<ObjectNode, M>> fn;
    try {
      fn = anoaHandler.functionChecked(rowFn(resultSet.getMetaData()));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return SQL.seq(resultSet, fn.compose(anoaHandler::ofNullable));
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
      for (int c = 0; c < n; ) {
        tb.writeFieldName(names[c++]);
        tb.writeTree(objectToTree(resultSet.getObject(c)));
      }
      tb.writeEndObject();
      return tb.asParser(objectCodec).readValueAsTree();
    };
  }

  private TreeNode objectToTree(Object object) throws IOException {
    if (object == null) {
      return NullNode.getInstance();
    }
    TokenBuffer tb = new TokenBuffer(objectCodec, false);
    try {
      tb.writeObject(object);
      return tb.asParser().readValueAsTree();
    } catch (IOException e) {
      tb = new TokenBuffer(objectCodec, false);
    }
    tb.writeString(object.toString());
    return tb.asParser().readValueAsTree();
  }
}
