/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models.transform_models;

import io.airbyte.protocol.models.StreamDescriptor;
import java.util.Objects;

public class RemoveStreamTransform {

  private final StreamDescriptor streamDescriptor;

  public RemoveStreamTransform(final StreamDescriptor streamDescriptor) {
    this.streamDescriptor = streamDescriptor;
  }

  @Override
  public String toString() {
    return "RemoveStreamTransform{" +
        "streamDescriptor=" + streamDescriptor +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RemoveStreamTransform)) {
      return false;
    }
    final RemoveStreamTransform that = (RemoveStreamTransform) o;
    return Objects.equals(streamDescriptor, that.streamDescriptor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamDescriptor);
  }

}
