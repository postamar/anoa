package com.adgear.anoa.read;

interface RecordWrapper<R, W extends FieldWrapper> {

  R get();

  void put(W fieldWrapper, Object value);

}
