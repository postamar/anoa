package com.adgear.anoa.factory;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

public class YamlObjects extends JacksonObjects<
    YAMLMapper,
    YAMLFactory,
    FormatSchema,
    YAMLParser,
    YAMLGenerator> {

  public YamlObjects() {
    super(new YAMLMapper());
  }
}
