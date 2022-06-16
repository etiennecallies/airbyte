/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models.transform_models;

import io.airbyte.protocol.models.JsonSchemaType;
import java.util.List;
import java.util.Objects;

public class AddFieldTransform {

  private final List<String> fieldName;
  private final List<JsonSchemaType> fieldType;

  public AddFieldTransform(final List<String> fieldName, final List<JsonSchemaType> fieldType) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
  }

  public List<JsonSchemaType> getFieldType() {
    return fieldType;
  }

  @Override
  public String toString() {
    return "AddFieldTransform{" +
        "fieldName=" + fieldName +
        ", fieldType=" + fieldType +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AddFieldTransform)) {
      return false;
    }
    final AddFieldTransform that = (AddFieldTransform) o;
    return Objects.equals(fieldName, that.fieldName) && Objects.equals(fieldType, that.fieldType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, fieldType);
  }

}
