/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;

public class PostgresUtils {

  private static final String PGOUTPUT_PLUGIN = "pgoutput";
  public static final Duration CDC_FIRST_RECORD_TIMEOUT = Duration.ofSeconds(5L);
  public static final Duration CDC_SUBSEQUENT_RECORD_TIMEOUT = Duration.ofSeconds(5L);

  public static String getPluginValue(final JsonNode field) {
    return field.has("plugin") ? field.get("plugin").asText() : PGOUTPUT_PLUGIN;
  }

}
