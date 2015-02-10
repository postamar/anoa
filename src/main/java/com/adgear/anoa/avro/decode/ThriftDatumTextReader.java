package com.adgear.anoa.avro.decode;

import com.adgear.anoa.avro.ThriftDataModified;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.thrift.ThriftData;
import org.apache.thrift.TBase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

public class ThriftDatumTextReader<T extends TBase<T,?>> extends GenericDatumTextReader<T> {

  public ThriftDatumTextReader(Class<T> thriftClass) {
    this(ThriftDataModified.getModified().getSchema(thriftClass));
  }

  public ThriftDatumTextReader(Schema reader) {
    this(reader, ThriftDataModified.getModified());
  }

  protected ThriftDatumTextReader(Schema reader, ThriftData data) {
    super(reader, data);
  }

  @Override
  protected Object createEnum(String symbol, Schema schema) {
    try {
      Class c = Class.forName(SpecificData.getClassName(schema));
      if (c == null) {
        return super.createEnum(symbol, schema); // punt to generic
      }
      return Enum.valueOf(c, symbol);
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  protected Object readInt(Schema s, Decoder in) throws IOException {
    int value = in.readInt();
    String type = s.getProp(ThriftDataModified.THRIFT_PROP);
    if (type != null) {
      switch (type) {
        case "byte": return ((byte) value);
        case "short": return ((short) value);
      }
    }
    return value;
  }

  @Override
  protected Collection newArray(int size, Schema schema) {
    if ("set".equals(schema.getProp(ThriftDataModified.THRIFT_PROP))) {
      return new HashSet();
    } else {
      return super.newArray(size, schema);
    }
  }

}
