/**
 *
 * Modified source code from the Apache Avro project, version 1.7.4 (http://avro.apache.org/)
 *
 *
 * LICENSE:
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *
 * NOTICE:
 *   Apache Avro
 *   Copyright 2010 The Apache Software Foundation
 *
 *   This product includes software developed at
 *   The Apache Software Foundation (http://www.apache.org/).
 *
 *   C JSON parsing provided by Jansson and
 *   written by Petri Lehtinen. The original software is
 *   available from http://www.digip.org/jansson/.
 */

package com.adgear.anoa.avro.decode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link DatumReader} implementation for deserializing Avro records from certain text formats.
 * Source code modified from {@link org.apache.avro.generic.GenericDatumReader}.
 *
 * @see org.apache.avro.generic.GenericDatumReader
 */
public class GenericDatumTextReader<D> implements DatumReader<D> {

  protected Schema schema;
  protected GenericData data;
  protected boolean bytesAsBase64 = false;
  protected boolean withFieldNames = false;
  protected Map<Schema, Map<String, Schema.Field>> aliasMap;
  protected Map<Schema.Field, Object> defaultMap;

  public GenericDatumTextReader(Schema schema) {
    this(schema, GenericData.get());
  }

  protected GenericDatumTextReader(Schema schema, GenericData data) {
    this.data = data;
    setSchema(schema);
    aliasMap = new HashMap<>();
    defaultMap = new HashMap<>();
    recursiveAliasCollect(schema);
  }

  private void recursiveAliasCollect(Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        recursiveAliasCollect(schema.getElementType());
        return;
      case MAP:
        recursiveAliasCollect(schema.getValueType());
        return;
      case UNION:
        for (Schema unionSchema : schema.getTypes()) {
          recursiveAliasCollect(unionSchema);
        }
        return;
      case RECORD:
        Map<String, Schema.Field> map = new HashMap<>();
        for (Schema.Field field : schema.getFields()) {
          map.put(field.name(), field);
          for (String alias : field.aliases()) {
            map.put(alias, field);
          }
          recursiveAliasCollect(field.schema());
        }
        aliasMap.put(schema, map);
    }
  }

  public GenericDatumTextReader<D> withoutBytesAsBase64() {
    bytesAsBase64 = false;
    return this;
  }

  public GenericDatumTextReader<D> withoutFieldNames() {
    withFieldNames = false;
    return this;
  }

  public GenericDatumTextReader<D> withBytesAsBase64() {
    bytesAsBase64 = true;
    return this;
  }

  public GenericDatumTextReader<D> withFieldNames() {
    withFieldNames = true;
    return this;
  }

  @Override
  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public D read(D reuse, Decoder in) throws IOException {
    try {
      final D datum = (D) recursiveRead(schema, in);
      return data.validate(schema, datum) ? datum : null;
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  protected Object recursiveRead(Schema expected, Decoder in) throws IOException {
    switch (expected.getType()) {
      case RECORD:
        return readRecord(expected, in);
      case ENUM:
        return readEnum(expected, in);
      case ARRAY:
        return readArray(expected, in);
      case MAP:
        return readMap(expected, in);
      case UNION:
        return readUnion(expected, in);
      case FIXED:
        return readFixed(expected, in);
      case STRING:
        return in.readString();
      case BYTES:
        return readBytes(in);
      case INT:
        return readInt(expected, in);
      case LONG:
        return in.readLong();
      case FLOAT:
        return in.readFloat();
      case DOUBLE:
        return in.readDouble();
      case BOOLEAN:
        return in.readBoolean();
      case NULL:
        in.readNull();
        return null;
      default:
        throw new IOException("Unknown type: " + expected);
    }
  }

  @SuppressWarnings("unchecked")
  protected Object getDefaultValue(Schema.Field field) {
    if (defaultMap.containsKey(field)) {
      return defaultMap.get(field);
    }
    if (field.defaultValue() == null) {
      defaultMap.put(field, null);
      return null;
    }
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      ResolvingGrammarGenerator.encode(encoder, field.schema(), field.defaultValue());
      encoder.flush();
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
      Object defaultValue = data.createDatumReader(field.schema()).read(null, decoder);
      defaultMap.put(field, defaultValue);
      return defaultValue;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Object readRecord(Schema expected, Decoder in) throws IOException {
    Object r = data.newRecord(null, expected);
    if (withFieldNames) {
      long l = in.readMapStart();
      boolean[] isSet = new boolean[expected.getFields().size()];
      if (l > 0) {
        do {
          for (int i = 0; i < l; i++) {
            final String fieldName = in.readString();
            final Schema.Field field = aliasMap.get(expected).get(fieldName);
            if (field == null) {
              in.readNull();
            } else {
              isSet[field.pos()] = true;
              data.setField(r, field.name(), field.pos(), recursiveRead(field.schema(), in));
            }
          }
        } while ((l = in.mapNext()) > 0);
      }
      for (Schema.Field field : expected.getFields()) {
        if (!isSet[field.pos()]) {
          if (field.defaultValue() == null) {
            throw new IOException("Field '" + field + "' not set.");
          }
          data.setField(r, field.name(), field.pos(),
                        data.deepCopy(field.schema(), getDefaultValue(field)));
        }
      }
    } else {
      for (Schema.Field field : expected.getFields()) {
        data.setField(r, field.name(), field.pos(), recursiveRead(field.schema(), in));
      }
    }
    return r;
  }

  protected Object readUnion(Schema expected, Decoder in) throws IOException {
    boolean isNull = (in.readIndex() == 0);
    if (isNull) {
      in.readNull();
      return null;
    } else {
      return recursiveReadUnion(expected, in);
    }
  }

  protected Object recursiveReadUnion(Schema schema, Decoder in) throws IOException {
    for (Schema unionSchema : schema.getTypes()) {
      switch (unionSchema.getType()) {
        case UNION:
          Object rValue = recursiveReadUnion(unionSchema, in);
          if (rValue != null) {
            return rValue;
          }
          break;
        case NULL:
          break;
        default:
          return recursiveRead(unionSchema, in);
      }
    }
    return null;
  }

  protected Object readEnum(Schema expected, Decoder in) throws IOException {
    final String str = in.readString();
    if (expected.hasEnumSymbol(str)) {
      return createEnum(str, expected);
    } else if (expected.hasEnumSymbol(str.toUpperCase())) {
      return createEnum(str.toUpperCase(), expected);
    } else if (expected.hasEnumSymbol(str.toLowerCase())) {
      return createEnum(str.toLowerCase(), expected);
    } else {
      try {
        return createEnum(expected.getEnumSymbols().get(Integer.parseInt(str)), expected);
      } catch (NumberFormatException | IndexOutOfBoundsException e) {
        throw new IOException(e);
      }
    }
  }

  protected Object createEnum(String symbol, Schema schema) {
    return new GenericData.EnumSymbol(schema, symbol);
  }

  protected Object readArray(Schema expected, Decoder in) throws IOException {
    Schema expectedType = expected.getElementType();
    long l = in.readArrayStart();
    Collection array = newArray((int) l, expected);
    if (l > 0) {
      long base = 0;
      do {
        for (long i = 0; i < l; i++) {
          array.add(recursiveRead(expectedType, in));
        }
        base += l;
      } while ((l = in.arrayNext()) > 0);
    }
    return array;
  }

  protected Collection newArray(int size, Schema schema) {
    return new GenericData.Array(size, schema);
  }

  protected Object readMap(Schema expected, Decoder in) throws IOException {
    Schema eValue = expected.getValueType();
    long l = in.readMapStart();
    HashMap<String, Object> map = new HashMap<>((int) l);
    if (l > 0) {
      do {
        for (int i = 0; i < l; i++) {
          String key = in.readString();
          map.put(key, recursiveRead(eValue, in));
        }
      } while ((l = in.mapNext()) > 0);
    }
    return map;
  }

  protected Object readInt(Schema s, Decoder in) throws IOException {
    return in.readInt();
  }

  protected Object readFixed(Schema expected, Decoder in) throws IOException {
    GenericFixed fixed = (GenericFixed) data.createFixed(null, expected);
    if (bytesAsBase64) {
      ByteBuffer
          .wrap(fixed.bytes(), 0, expected.getFixedSize())
          .put(Base64.decodeBase64(in.readString()));
    } else {
      in.readFixed(fixed.bytes(), 0, expected.getFixedSize());
    }
    return fixed;
  }

  protected Object readBytes(Decoder in) throws IOException {
    if (bytesAsBase64) {
      return ByteBuffer.wrap(Base64.decodeBase64(in.readString()));
    } else {
      return in.readBytes(null);
    }
  }
}
