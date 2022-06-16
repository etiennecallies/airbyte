/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models.transform_models;

import io.airbyte.protocol.models.JsonSchemaType;
import java.util.List;
import java.util.Objects;

public class UpdateFieldTransform {

  private final List<String> fieldName;
  private final List<JsonSchemaType> oldType;
  private final List<JsonSchemaType> newType;

  public UpdateFieldTransform(final List<String> fieldName, final List<JsonSchemaType> oldType, final List<JsonSchemaType> newType) {
    this.fieldName = fieldName;
    this.oldType = oldType;
    this.newType = newType;
  }

  public List<JsonSchemaType> getOldType() {
    return oldType;
  }

  public List<JsonSchemaType> getNewType() {
    return newType;
  }

  @Override
  public String toString() {
    return "UpdateFieldTransform{" +
        "fieldName=" + fieldName +
        ", oldType=" + oldType +
        ", newType=" + newType +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UpdateFieldTransform)) {
      return false;
    }
    final UpdateFieldTransform that = (UpdateFieldTransform) o;
    return Objects.equals(fieldName, that.fieldName) && Objects.equals(oldType, that.oldType) && Objects.equals(newType,
        that.newType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, oldType, newType);
  }

}
