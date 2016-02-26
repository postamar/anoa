package com.adgear.anoa.parser.type;

import com.adgear.anoa.parser.SchemaGenerator;

import java.util.Optional;

abstract class WrapperBase implements FieldType {

  final FieldType wrapped;

  protected WrapperBase(FieldType wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public Optional<SchemaGenerator> getDependency() {
    return wrapped.getDependency();
  }
}
