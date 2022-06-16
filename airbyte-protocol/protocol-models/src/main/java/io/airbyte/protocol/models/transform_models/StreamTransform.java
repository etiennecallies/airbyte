/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models.transform_models;

import io.airbyte.protocol.models.StreamDescriptor;

public record StreamTransform(StreamTransformType transformType,
                              AddStreamTransform addStreamTransform,
                              RemoveStreamTransform removeStreamTransform,
                              UpdateStreamTransform updateStreamTransform) {

  public static StreamTransform createAddStreamTransform(final StreamDescriptor streamDescriptor) {
    return createAddStreamTransform(new AddStreamTransform(streamDescriptor));
  }

  public static StreamTransform createAddStreamTransform(final AddStreamTransform addStreamTransform) {
    return new StreamTransform(StreamTransformType.ADD_STREAM, addStreamTransform, null, null);
  }

  public static StreamTransform createRemoveStreamTransform(final StreamDescriptor streamDescriptor) {
    return createRemoveStreamTransform(new RemoveStreamTransform(streamDescriptor));
  }

  public static StreamTransform createRemoveStreamTransform(final RemoveStreamTransform removeStreamTransform) {
    return new StreamTransform(StreamTransformType.REMOVE_STREAM, null, removeStreamTransform, null);
  }

  public static StreamTransform createUpdateStreamTransform(final UpdateStreamTransform updateStreamTransform) {
    return new StreamTransform(StreamTransformType.UPDATE_STREAM, null, null, updateStreamTransform);
  }

  public StreamTransformType getTransformType() {
    return transformType;
  }

  public AddStreamTransform getAddStreamTransform() {
    return addStreamTransform;
  }

  public RemoveStreamTransform getRemoveStreamTransform() {
    return removeStreamTransform;
  }

  public UpdateStreamTransform getUpdateStreamTransform() {
    return updateStreamTransform;
  }

  @Override
  public String toString() {
    return "StreamTransform{" +
        "transformType=" + transformType +
        ", addStreamTransform=" + addStreamTransform +
        ", removeStreamTransform=" + removeStreamTransform +
        ", updateStreamTransform=" + updateStreamTransform +
        '}';
  }

}
