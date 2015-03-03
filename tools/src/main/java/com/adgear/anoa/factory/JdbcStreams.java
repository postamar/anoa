package com.adgear.anoa.factory;

import checkers.nullness.quals.NonNull;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.apache.avro.Schema;
import org.apache.commons.codec.digest.DigestUtils;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.jooq.lambda.SQL;
import org.jooq.lambda.Unchecked;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class JdbcStreams {

  static private ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static public @NonNull Stream<ObjectNode> from(@NonNull ResultSet resultSet) {
    try {
      return SQL.seq(resultSet, rowReadFn(resultSet.getMetaData()));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

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
    Schema avroSchema = Schema.createRecord("jdbc_" + DigestUtils.shaHex(fields.toString()),
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

  static protected Function<ResultSet, ObjectNode> rowReadFn(ResultSetMetaData rsmd)
      throws SQLException {
    List<BiConsumer<JsonGenerator, ResultSet>> columnBiCoList = IntStream
        .range(1, rsmd.getColumnCount() + 1)
        .mapToObj(Unchecked.intFunction(c -> {
          String columnName = rsmd.getColumnLabel(c);
          return Unchecked.biConsumer((JsonGenerator jg, ResultSet rs) -> {
            jg.writeFieldName(columnName);
            jg.writeObject(rs.getObject(c));
          });
        }))
        .collect(Collectors.toList());

    return Unchecked.function(rs -> {
      try (TokenBuffer tokenBuffer = new TokenBuffer(OBJECT_MAPPER, false)) {
        tokenBuffer.writeStartObject();
        columnBiCoList.stream().forEach(bico -> bico.accept(tokenBuffer, rs));
        tokenBuffer.writeEndObject();
        return tokenBuffer.asParser(OBJECT_MAPPER).readValueAsTree();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

}
