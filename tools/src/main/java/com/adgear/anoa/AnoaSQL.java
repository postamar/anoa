package com.adgear.anoa;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

import org.jooq.lambda.SQL;
import org.jooq.lambda.Seq;
import org.jooq.lambda.Unchecked;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.function.BiConsumer;

public class AnoaSQL {

  static public Seq<TokenBuffer> stream(ResultSet resultSet) throws SQLException {
    ObjectMapper objectMapper = new ObjectMapper();
    BiConsumer<JsonGenerator,ResultSet> bico = rowReadBiConsumer(resultSet.getMetaData());
    return SQL.seq(resultSet, rs -> {
      TokenBuffer tb = new TokenBuffer(objectMapper, false);
      bico.accept(tb, rs);
      try {
        tb.flush();
      } catch(IOException e) {
        throw new UncheckedIOException(e);
      }
      return tb;
    });
  }

  static public BiConsumer<JsonGenerator, ResultSet> rowReadBiConsumer(ResultSetMetaData rsmd)
      throws SQLException {
    String[] columnNames = new String[rsmd.getColumnCount()];
    for (int c = 0; c < columnNames.length; c++) {
      columnNames[c] = rsmd.getColumnName(c + 1);
    }
    return Unchecked.biConsumer((jg, rs) -> {
      jg.writeStartObject();
      int c = 0;
      for (String columnName : columnNames) {
        jg.writeFieldName(columnName);
        jg.writeObject(rs.getObject(++c));
      }
      jg.writeEndObject();
    });
  }
}
