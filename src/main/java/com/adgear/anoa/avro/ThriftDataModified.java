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

package com.adgear.anoa.avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.thrift.ThriftData;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ThriftData modified with a few hacks.
 */
public class ThriftDataModified extends ThriftData {

  static private final Map<String, TBase> recordCache = new HashMap<>();
  static private final ThriftDataModified INSTANCE = new ThriftDataModified();
  static public final Schema NULL = Schema.create(Schema.Type.NULL);
  static public final String THRIFT_PROP = "thrift";

  static public ThriftDataModified getModified() {
    return INSTANCE;
  }

  @Override
  public Object newRecord(Object old, Schema schema) {
    final String recordName = schema.getFullName();
    TBase cachedRecord = recordCache.get(recordName);
    if (cachedRecord == null) {
      final Object record = super.newRecord(old, schema);
      if (record instanceof TBase) {
        cachedRecord = (TBase) record;
        recordCache.put(recordName, cachedRecord);
      } else {
        return record;
      }
    }
    return cachedRecord.deepCopy();
  }

  /**
   * Return a record schema given a thrift generated class.
   */
  @SuppressWarnings("unchecked")
  @Override
  public Schema getSchema(Class c) {
    Schema schema;
    try {
      if (TEnum.class.isAssignableFrom(c)) {    // enum
        List<String> symbols = new ArrayList<String>();
        for (Enum e : ((Class<? extends Enum>) c).getEnumConstants()) {
          symbols.add(e.name());
        }
        schema = Schema.createEnum(c.getName(), null, null, symbols);
      } else if (TBase.class.isAssignableFrom(c)) { // struct
        schema = Schema.createRecord(c.getName(), null, null, Throwable.class.isAssignableFrom(c));
        List<Schema.Field> fields = new ArrayList<>();
        for (FieldMetaData f : FieldMetaData.getStructMetaDataMap(c).values()) {
          Schema s = getSchema(f.valueMetaData);
          JsonNode defaultValue = null;
          if (f.requirementType == TFieldRequirementType.OPTIONAL) {
            if (s.getType() != Schema.Type.UNION) {
              s = nullable(s);
            }
            defaultValue = NullNode.getInstance();
          }
          fields.add(new Schema.Field(f.fieldName, s, null, defaultValue));
        }
        schema.setFields(fields);
      } else {
        throw new RuntimeException("Not a Thrift-generated class: " + c);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return schema;
  }

  private Schema getSchema(FieldValueMetaData f) {
    switch (f.type) {
      case TType.BOOL:
        return Schema.create(Schema.Type.BOOLEAN);
      case TType.BYTE:
        Schema b = Schema.create(Schema.Type.INT);
        b.addProp(THRIFT_PROP, "byte");
        return b;
      case TType.I16:
        Schema s = Schema.create(Schema.Type.INT);
        s.addProp(THRIFT_PROP, "short");
        return s;
      case TType.I32:
        return Schema.create(Schema.Type.INT);
      case TType.I64:
        return Schema.create(Schema.Type.LONG);
      case TType.DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case TType.ENUM:
        EnumMetaData enumMeta = (EnumMetaData) f;
        return nullable(getSchema(enumMeta.enumClass));
      case TType.LIST:
        ListMetaData listMeta = (ListMetaData) f;
        return nullable(Schema.createArray(getSchema(listMeta.elemMetaData)));
      case TType.MAP:
        MapMetaData mapMeta = (MapMetaData) f;
        if (mapMeta.keyMetaData.type != TType.STRING) {
          throw new AvroRuntimeException("Map keys must be strings: " + f);
        }
        Schema map = Schema.createMap(getSchema(mapMeta.valueMetaData));
        GenericData.setStringType(map, GenericData.StringType.String);
        return nullable(map);
      case TType.SET:
        SetMetaData setMeta = (SetMetaData) f;
        Schema set = Schema.createArray(getSchema(setMeta.elemMetaData));
        set.addProp(THRIFT_PROP, "set");
        return nullable(set);
      case TType.STRING:
        if (f.isBinary()) {
          return nullable(Schema.create(Schema.Type.BYTES));
        }
        Schema string = Schema.create(Schema.Type.STRING);
        GenericData.setStringType(string, GenericData.StringType.String);
        return nullable(string);
      case TType.STRUCT:
        StructMetaData structMeta = (StructMetaData) f;
        Schema record = getSchema(structMeta.structClass);
        return nullable(record);
      case TType.VOID:
        return NULL;
      default:
        throw new RuntimeException("Unexpected type in field: " + f);
    }
  }

  @SuppressWarnings("unchecked")
  private Schema nullable(Schema schema) {
    return Schema.createUnion(Arrays.asList(NULL, schema));
  }
}
