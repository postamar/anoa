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

package com.adgear.anoa.avro.encode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.Encoder;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Extension to {@link org.apache.avro.generic.GenericDatumWriter} allowing more control on how
 * certain fields are serialized. Source code modified from {@link org.apache.avro.generic.GenericDatumWriter}.
 *
 * @see org.apache.avro.generic.GenericDatumWriter
 */
public class GenericDatumTextWriter<D> extends GenericDatumWriter<D> {

  protected boolean enumsAsString = false;
  protected boolean bytesAsBase64 = false;
  protected boolean withFieldNames = false;

  public GenericDatumTextWriter() {
    super();
  }

  protected GenericDatumTextWriter(GenericData data) {
    super(data);
  }

  public GenericDatumTextWriter(Schema root) {
    super(root);
  }

  public GenericDatumTextWriter(Schema root, GenericData data) {
    super(root, data);
  }

  public GenericDatumTextWriter<D> withoutEnumsAsString() {
    enumsAsString = false;
    return this;
  }

  public GenericDatumTextWriter<D> withoutBytesAsBase64() {
    bytesAsBase64 = false;
    return this;
  }

  public GenericDatumTextWriter<D> withoutFieldNames() {
    withFieldNames = false;
    return this;
  }

  public GenericDatumTextWriter<D> withEnumsAsString() {
    enumsAsString = true;
    return this;
  }

  public GenericDatumTextWriter<D> withBytesAsBase64() {
    bytesAsBase64 = true;
    return this;
  }

  public GenericDatumTextWriter<D> withFieldNames() {
    withFieldNames = true;
    return this;
  }

  @Override
  protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
    if (!enumsAsString) {
      super.writeEnum(schema, datum, out);
    } else {
      writeString(datum.toString(), out);
    }
  }

  @Override
  protected void writeBytes(Object datum, Encoder out) throws IOException {
    if (!bytesAsBase64) {
      super.writeBytes(datum, out);
    } else {
      ByteBuffer bytes = (ByteBuffer) datum;
      byte[] b = new byte[bytes.remaining()];
      bytes.get(b);
      writeString(Base64.encodeBase64String(b), out);
    }
  }

  @Override
  protected void writeFixed(Schema schema, Object datum, Encoder out) throws IOException {
    if (!bytesAsBase64) {
      super.writeFixed(schema, datum, out);
    } else {
      writeBytes(ByteBuffer.wrap(((GenericFixed) datum).bytes(), 0, schema.getFixedSize()), out);
    }
  }

  @Override
  protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
    if (!withFieldNames) {
      super.writeRecord(schema, datum, out);
      out.flush();
    } else {
      out.writeMapStart();
      out.setItemCount(schema.getFields().size());
      for (Schema.Field f : schema.getFields()) {
        out.startItem();
        out.writeString(f.name());
        Object value = getData().getField(datum, f.name(), f.pos());
        try {
          write(f.schema(), value, out);
        } catch (NullPointerException e) {
          throw npe(e, " in field " + f.name());
        }
      }
      out.writeMapEnd();
      out.flush();
    }
  }
}
