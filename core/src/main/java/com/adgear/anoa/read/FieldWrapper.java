package com.adgear.anoa.read;

import java.util.stream.Stream;

interface FieldWrapper {

  Stream<String> getNames();

  AbstractReader<?> getReader();

  boolean equalsDefaultValue(Object value);
}
