package com.adgear.anoa.tools.function;

import checkers.nullness.quals.NonNull;

import com.adgear.anoa.AnoaRecord;
import com.adgear.anoa.impl.AnoaFunctionBase;

import org.jooq.lambda.Unchecked;

import java.lang.reflect.Field;
import java.util.stream.Stream;


public class AnoaFieldNuller<T> extends AnoaFunctionBase<T, T> {

  final protected String[] fields;

  public Stream<String> getFields() {
    return Stream.of(fields);
  }

  public AnoaFieldNuller(String... fields) {
    this.fields = fields;
  }

  @Override
  protected AnoaRecord<T> applyPresent(@NonNull AnoaRecord<T> record) {
    final T object = record.asOptional().get();
    Stream.of(fields)
        .map(Unchecked.function(name -> getFieldAtPath(object, name)))
        .forEach(Unchecked.consumer(wrapper -> wrapper.field.set(wrapper.object, null)));
    return record;
  }

  protected Object getObjectAtPath(Object o, String[] path, int startIndex)
      throws NoSuchFieldException, IllegalAccessException {
    if (startIndex >= path.length - 1) {
      return o;
    } else {
      Field f = o.getClass().getDeclaredField(path[startIndex]);
      return getObjectAtPath(f.get(o), path, startIndex + 1);
    }
  }
  /* Will either return a Field object if `name' describes a proper path
    * leading to a field, or null if one of the components leading to the field
    * is set to null (trying to access a field that does not exist at all,
    * rather than being null, will trigger an exception). */
  protected ObjectFieldWrapper getFieldAtPath(Object o, String name)
      throws NoSuchFieldException, IllegalAccessException {
    String[] components = name.split("\\.");
    String id = components[components.length - 1];

    Object objectAtPath = getObjectAtPath(o, components, 0);
    return (objectAtPath == null)
           ? null
           : new ObjectFieldWrapper(objectAtPath, objectAtPath.getClass().getField(id));
  }

  static protected class ObjectFieldWrapper {

    final Object object;
    final Field field;

    ObjectFieldWrapper(Object object, Field field) {
      this.object = object;
      this.field = field;
    }
  }
}
