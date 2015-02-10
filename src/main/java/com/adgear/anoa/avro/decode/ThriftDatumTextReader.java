package com.adgear.anoa.avro.decode;

import com.adgear.anoa.avro.ThriftDataModified;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificData;
import org.apache.thrift.TBase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ThriftDatumTextReader<T extends TBase<T,?>> extends GenericDatumTextReader<T> {

  public ThriftDatumTextReader(Class<T> thriftClass) {
    this(ThriftDataModified.getModified().getSchema(thriftClass));
  }

  public ThriftDatumTextReader(Schema reader) {
    this(reader, ThriftDataModified.getModified());
  }

  protected ThriftDatumTextReader(Schema reader, ThriftDataModified data) {
    super(reader, data);
  }

  final private Map<String,Class> enumClassCache = new HashMap<>();
  final private Map<String,Enum> enumSymbolCache = new HashMap<>();

  @Override
  protected Object createEnum(String symbol, Schema schema) {
    final String enumClassName = SpecificData.getClassName(schema);
    if (!enumClassCache.containsKey(enumClassName)) {
      try {
        enumClassCache.put(enumClassName, Class.forName(enumClassName));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    final Class c = enumClassCache.get(enumClassName);
    if (c == null) {
      return super.createEnum(symbol, schema); // punt to generic
    } else {
      final String enumSymbolName = enumClassName + '.' + symbol;
      Enum value = enumSymbolCache.get(enumSymbolName);
      if (value == null) {
        value = Enum.valueOf(c, symbol);
        enumSymbolCache.put(enumSymbolName, value);
      }
      return value;
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
