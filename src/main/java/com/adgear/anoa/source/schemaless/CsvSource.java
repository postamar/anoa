package com.adgear.anoa.source.schemaless;

import com.adgear.anoa.provider.base.CounterlessProviderBase;
import com.adgear.anoa.source.Source;

import org.apache.avro.Schema;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Iterates over comma-separated values, exposed as String lists.
 *
 * @see com.adgear.anoa.source.schemaless.CsvWithHeaderSource
 */
public class CsvSource
    extends CounterlessProviderBase<List<String>> implements Source<List<String>> {

  final protected CsvListReader csvListReader;
  final protected Schema avroSchema;
  protected List<String> buffer = null;

  public CsvSource(Reader in) {
    this(in, false);
  }

  protected CsvSource(Reader in, boolean hasHeader) {
    this(in, CsvPreference.STANDARD_PREFERENCE, hasHeader);
  }

  protected CsvSource(Reader in, CsvPreference preference, boolean withHeader) {
    csvListReader = new CsvListReader(in, preference);
    String[] header = null;
    if (withHeader) {
      try {
        header = csvListReader.getHeader(true);
      } catch (IOException e) {
        logger.error(e.getMessage());
      }
    }
    avroSchema = (withHeader && header != null) ? inferSchema(header) : null;
  }


  @Override
  protected List<String> getNext() throws IOException {
    List<String> rValue = (buffer == null) ? csvListReader.read() : buffer;
    buffer = null;
    return rValue;
  }

  @Override
  public boolean hasNext() {
    if (buffer != null) {
      return true;
    }
    try {
      buffer = csvListReader.read();
    } catch (IOException e) {
      logger.error(e.getMessage());
    }
    return (buffer != null);
  }

  @Override
  public void close() throws IOException {
    csvListReader.close();
  }

  private Schema inferSchema(String[] header) {
    ArrayList<Schema.Field> fields = new ArrayList<Schema.Field>();
    int i = 0;
    StringBuilder sb = new StringBuilder();
    for (String name : header) {
      Schema[] union = new Schema[2];
      union[0] = Schema.create(Schema.Type.NULL);
      union[1] = Schema.create(Schema.Type.STRING);
      Schema.Field field = new Schema.Field(name,
                                            Schema.createUnion(Arrays.asList(union)),
                                            "induced from column " + (++i),
                                            JsonNodeFactory.instance.nullNode());
      fields.add(field);
      sb.append(field.toString());
    }
    Schema schema = Utils.createSchema("csv", sb.toString());
    schema.setFields(fields);
    return schema;
  }
}
