/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models.transform_models;

import io.airbyte.protocol.models.StreamDescriptor;
import java.util.Objects;
import java.util.Set;

public class UpdateStreamTransform {

  private final StreamDescriptor streamDescriptor;
  private final Set<FieldTransform> fieldTransforms;

  public UpdateStreamTransform(final StreamDescriptor streamDescriptor, final Set<FieldTransform> fieldTransforms) {
    this.streamDescriptor = streamDescriptor;
    this.fieldTransforms = fieldTransforms;
  }

  public Set<FieldTransform> getFieldTransforms() {
    return fieldTransforms;
  }

  public StreamDescriptor getStreamDescriptor() {
    return streamDescriptor;
  }

  @Override
  public String toString() {
    return "UpdateStreamTransform{" +
        "streamDescriptor=" + streamDescriptor +
        ", fieldTransforms=" + fieldTransforms +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UpdateStreamTransform)) {
      return false;
    }
    final UpdateStreamTransform that = (UpdateStreamTransform) o;
    return Objects.equals(streamDescriptor, that.streamDescriptor) && Objects.equals(fieldTransforms, that.fieldTransforms);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamDescriptor, fieldTransforms);
  }

}
