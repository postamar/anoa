package com.adgear.anoa.read;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.node.NullNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.Stream;


class AvroFieldWrapper implements FieldWrapper {

  final int index;
  final Schema.Field field;
  final boolean unboxed;
  final Object defaultValue;
  final AbstractReader<?> reader;

  AvroFieldWrapper(int index, Schema.Field field) {
    this.index = index;
    this.field = field;
    this.reader = createReader(field.schema());
    this.defaultValue = readDefaultValue(field);
    switch (field.schema().getType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        this.unboxed = true;
        break;
      default:
        this.unboxed = false;
    }
  }

  @Override
  public Stream<String> getNames() {
    return Stream.concat(Stream.of(field.name()), field.aliases().stream());
  }

  @Override
  public AbstractReader<?> getReader() {
    return reader;
  }

  @SuppressWarnings("unchecked")
  static private AbstractReader<?> createReader(Schema schema) {
    switch (schema.getType()) {
      case ARRAY:
        return new ListReader(createReader(schema.getElementType()));
      case BOOLEAN:
        return new BooleanReader();
      case BYTES:
        return new ByteBufferReader();
      case DOUBLE:
        return new DoubleReader();
      case ENUM:
        return new EnumReader(SpecificData.get().getClass(schema));
      case FIXED:
        final Class<? extends SpecificFixed> fixedClass = SpecificData.get().getClass(schema);
        return (fixedClass == null)
               ? new AvroFixedReader.AvroGenericFixedReader(schema)
               : new AvroFixedReader.AvroSpecificFixedReader<>(fixedClass);
      case FLOAT:
        return new FloatReader();
      case INT:
        return new IntegerReader();
      case LONG:
        return new LongReader();
      case MAP:
        return new MapReader(createReader(schema.getValueType()));
      case RECORD:
        final Class<? extends SpecificRecord> recordClass = SpecificData.get().getClass(schema);
        return (recordClass == null)
               ? new AvroReader.GenericReader(schema)
               : new AvroReader.SpecificReader<>(recordClass);
      case STRING:
        return new StringReader();
      case UNION:
        if (schema.getTypes().size() == 2) {
          return createReader(schema.getTypes().get(
              (schema.getTypes().get(0).getType() == Schema.Type.NULL) ? 1 : 0));
        }
    }
    throw new RuntimeException("Unsupported Avro schema: " + schema);
  }

  Object defaultValueCopy() {
    return SpecificData.get().deepCopy(field.schema(), defaultValue);
  }

  @SuppressWarnings("unchecked")
  private Object readDefaultValue(Schema.Field field) {
    if (field.defaultValue() == null || NullNode.getInstance().equals(field.defaultValue())) {
      return null;
    }
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      ResolvingGrammarGenerator.encode(encoder, field.schema(), field.defaultValue());
      encoder.flush();
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
      return SpecificData.get().createDatumReader(field.schema()).read(null, decoder);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }
}
