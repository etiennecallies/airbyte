/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models.transform_models;

import io.airbyte.protocol.models.JsonSchemaType;
import java.util.List;

public record FieldTransform(FieldTransformType transformType,
                             AddFieldTransform addFieldTransform,
                             RemoveFieldTransform removeFieldTransform,
                             UpdateFieldTransform updateFieldTransform) {

  public static FieldTransform createAddFieldTransform(final List<String> fieldName, final List<JsonSchemaType> fieldType) {
    return createAddFieldTransform(new AddFieldTransform(fieldName, fieldType));
  }

  public static FieldTransform createAddFieldTransform(final AddFieldTransform addFieldTransform) {
    return new FieldTransform(FieldTransformType.ADD_FIELD, addFieldTransform, null, null);
  }

  public static FieldTransform createRemoveFieldTransform(final List<String> fieldName, final List<JsonSchemaType> fieldType) {
    return createRemoveFieldTransform(new RemoveFieldTransform(fieldName, fieldType));
  }

  public static FieldTransform createRemoveFieldTransform(final RemoveFieldTransform removeFieldTransform) {
    return new FieldTransform(FieldTransformType.REMOVE_FIELD, null, removeFieldTransform, null);
  }

  public static FieldTransform createUpdateFieldTransform(final UpdateFieldTransform updateFieldTransform) {
    return new FieldTransform(FieldTransformType.UPDATE_FIELD, null, null, updateFieldTransform);
  }

  public FieldTransformType getTransformType() {
    return transformType;
  }

  public AddFieldTransform getAddFieldTransform() {
    return addFieldTransform;
  }

  public RemoveFieldTransform getRemoveFieldTransform() {
    return removeFieldTransform;
  }

  public UpdateFieldTransform getUpdateFieldTransform() {
    return updateFieldTransform;
  }

  @Override
  public String toString() {
    return "FieldTransform{" +
        "transformType=" + transformType +
        ", addFieldTransform=" + addFieldTransform +
        ", removeFieldTransform=" + removeFieldTransform +
        ", updateFieldTransform=" + updateFieldTransform +
        '}';
  }

}
