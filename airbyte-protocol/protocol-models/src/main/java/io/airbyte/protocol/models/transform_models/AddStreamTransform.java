/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models.transform_models;

import io.airbyte.protocol.models.StreamDescriptor;
import java.util.Objects;

public class AddStreamTransform {

  private final StreamDescriptor streamDescriptor;

  public AddStreamTransform(final StreamDescriptor streamDescriptor) {
    this.streamDescriptor = streamDescriptor;
  }

  public StreamDescriptor getStreamDescriptor() {
    return streamDescriptor;
  }

  @Override
  public String toString() {
    return "AddStreamTransform{" +
        "streamDescriptor=" + streamDescriptor +
        '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AddStreamTransform)) {
      return false;
    }
    final AddStreamTransform that = (AddStreamTransform) o;
    return Objects.equals(streamDescriptor, that.streamDescriptor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(streamDescriptor);
  }

}
