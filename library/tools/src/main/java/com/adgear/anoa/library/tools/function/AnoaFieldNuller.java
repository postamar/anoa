package com.adgear.anoa.library.tools.function;

import org.jooq.lambda.fi.util.function.CheckedUnaryOperator;

import java.lang.reflect.Field;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * A UnaryOperator implementation which sets specified object properties to null.
 *
 * @param <T> Value type
 */
public class AnoaFieldNuller<T> implements UnaryOperator<T> {

  final protected String[] fields;

  /**
   * @param fields the field names to set to null, according to this object
   */
  public AnoaFieldNuller(String... fields) {
    this.fields = fields;
  }

  /**
   * @return the field names to set to null, according to this object
   */
  public Stream<String> getFields() {
    return Stream.of(fields);
  }


  public T applyChecked(T object)
      throws NoSuchFieldException, IllegalAccessException {
    for (String field : fields) {
      ObjectFieldWrapper wrapper = getFieldAtPath(object, field);
      if (wrapper != null) {
        wrapper.field.set(wrapper.object, null);
      }
    }
    return object;
  }

  public T apply(T object) {
    try {
      return applyChecked(object);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public CheckedUnaryOperator<T> asChecked() {
    return this::applyChecked;
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
