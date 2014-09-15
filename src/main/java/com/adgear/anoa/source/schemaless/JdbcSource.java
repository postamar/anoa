package com.adgear.anoa.source.schemaless;

import com.adgear.anoa.provider.base.CounterlessProviderBase;
import com.adgear.anoa.source.avro.AvroSource;

import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.node.JsonNodeFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Iterates over a JDBC ResultSet, exposes each record as a String list.
 *
 * @see com.adgear.anoa.source.avro.AvroSource
 */
public class JdbcSource
    extends CounterlessProviderBase<List<String>> implements AvroSource<List<String>> {

  final protected ResultSet resultSet;
  final protected Schema.Type avroTypes[];
  final private Schema avroSchema;
  private List<String> buffer = null;

  public JdbcSource(ResultSet resultSet) {
    this.resultSet = resultSet;
    // infer column types and build schema
    ArrayList<Schema.Field> fields = new ArrayList<>();
    try {
      ResultSetMetaData metaData = resultSet.getMetaData();
      this.avroTypes = inferAvroColumnTypes(metaData);
      for (int i = 0; i < avroTypes.length; i++) {
        fields.add(buildField(avroTypes[i], metaData.getColumnLabel(i + 1)));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    this.avroSchema = Utils.createSchema("jdbc", fields.toString());
    this.avroSchema.setFields(fields);
  }

  static Schema.Type getAvroType(int sqlType) {
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

  private Schema.Type[] inferAvroColumnTypes(ResultSetMetaData metaData) throws SQLException {
    Schema.Type[] types = new Schema.Type[metaData.getColumnCount()];
    for (int i = 0; i < metaData.getColumnCount(); i++) {
      types[i] = getAvroType(metaData.getColumnType(i + 1));
    }
    return types;
  }

  private Schema.Field buildField(Schema.Type type, String label) {
    Schema[] union = new Schema[2];
    union[0] = Schema.create(Schema.Type.NULL);
    union[1] = Schema.create(type);
    return new Schema.Field(label,
                            Schema.createUnion(Arrays.asList(union)),
                            null,
                            JsonNodeFactory.instance.nullNode());

  }

  /**
   * @return the Avro Schema induced from the corresponding JDBC ResultSetMetadata object.
   */
  @Override
  public Schema getAvroSchema() {
    return avroSchema;
  }

  @Override
  public void close() throws IOException {
    try {
      resultSet.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean hasNext() {
    if (buffer == null) {
      buffer = nextRow();
    }
    return (buffer != null);
  }

  @Override
  protected List<String> getNext() {
    List<String> rValue = (buffer == null) ? nextRow() : buffer;
    buffer = null;
    return rValue;
  }

  protected List<String> nextRow() {
    try {
      if (!resultSet.next()) {
        return null;
      }
      ArrayList<String> list = new ArrayList<String>(avroTypes.length);
      for (int i = 0; i < avroTypes.length; i++) {
        Object object = resultSet.getObject(i + 1);
        String value = null;
        if (object != null) {
          switch (avroTypes[i]) {
            case BYTES:
              value = new String(Base64.encodeBase64(resultSet.getBytes(i + 1), false));
              break;
            case BOOLEAN:
              value = Boolean.toString(resultSet.getBoolean(i + 1));
              break;
          }
          if (value == null) {
            value = object.toString();
          }
        }
        list.add(value);
      }
      return list;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
