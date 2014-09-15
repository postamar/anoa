package com.adgear.anoa.tools.codec;

import com.adgear.anoa.codec.base.CounterlessCodecBase;
import com.adgear.anoa.provider.Provider;

import org.apache.avro.generic.GenericContainer;

import java.lang.reflect.Field;

/**
 * Transforms each provided record by <code>null</code>-ing several of its fields.
 *
 * @param <R> A class with a matching Avro schema (as far as java reflection is concerned).
 */
public class CleanserCodec<R extends GenericContainer> extends CounterlessCodecBase<R, R> {

  private String[] fields;

  /**
   * @param fields name of the fields to be nulled.
   */
  public CleanserCodec(Provider<R> provider, String[] fields) {
    super(provider);
    this.fields = fields;
  }

  /**
   * @return the same input object, after having nulled some of its fields.
   */
  @Override
  public R transform(R input) {
    try {
      for (String id : fields) {
        ObjectField field = getFieldAtPath(input, id);
        if (field != null) {
          field.getField().set(field.getObject(), null);
        }
      }
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
    return input;
  }

  private Object getObjectAtPath(Object o, String[] path, int startIndex)
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
  private ObjectField getFieldAtPath(Object o, String name)
      throws NoSuchFieldException, IllegalAccessException {
    String[] components = name.split("\\.");
    String id = components[components.length - 1];

    Object objectAtPath = getObjectAtPath(o, components, 0);
    if (objectAtPath == null) {
      return null;
    } else {
      return new ObjectField(objectAtPath, objectAtPath.getClass().getField(id));
    }
  }

  private class ObjectField {

    Object object;
    Field field;

    public ObjectField(Object object, Field field) {
      this.object = object;
      this.field = field;
    }

    public Object getObject() {
      return object;
    }

    public Field getField() {
      return field;
    }
  }

}
