/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.protocol.models.transform_models.FieldTransform;
import io.airbyte.protocol.models.transform_models.StreamTransform;
import io.airbyte.protocol.models.transform_models.UpdateFieldTransform;
import io.airbyte.protocol.models.transform_models.UpdateStreamTransform;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class CatalogHelpersTest {

  @Test
  void testFieldToJsonSchema() {
    final String expected = """
                                {
                                  "type": "object",
                                  "properties": {
                                    "name": {
                                      "type": "string"
                                    },
                                    "test_object": {
                                      "type": "object",
                                      "properties": {
                                        "thirdLevelObject": {
                                          "type": "object",
                                          "properties": {
                                            "data": {
                                              "type": "string"
                                            },
                                            "intData": {
                                              "type": "number"
                                            }
                                          }
                                        },
                                        "name": {
                                          "type": "string"
                                        }
                                      }
                                    }
                                  }
                                }
                            """;
    final JsonNode actual = CatalogHelpers.fieldsToJsonSchema(Field.of("name", JsonSchemaType.STRING),
        Field.of("test_object", JsonSchemaType.OBJECT, List.of(
            Field.of("name", JsonSchemaType.STRING),
            Field.of("thirdLevelObject", JsonSchemaType.OBJECT, List.of(
                Field.of("data", JsonSchemaType.STRING),
                Field.of("intData", JsonSchemaType.NUMBER))))));

    assertEquals(Jsons.deserialize(expected), actual);
  }

  @Test
  void testGetTopLevelFieldNames() {
    final String json = "{ \"type\": \"object\", \"properties\": { \"name\": { \"type\": \"string\" } } } ";
    final Set<String> actualFieldNames =
        CatalogHelpers.getTopLevelFieldNames(new ConfiguredAirbyteStream().withStream(new AirbyteStream().withJsonSchema(Jsons.deserialize(json))));

    assertEquals(Sets.newHashSet("name"), actualFieldNames);
  }

  @Test
  void testGetFieldNames() throws IOException {
    final JsonNode node = Jsons.deserialize(MoreResources.readResource("valid_schema.json"));
    final Set<String> actualFieldNames = CatalogHelpers.getAllFieldNames(node);
    final Set<String> expectedFieldNames =
        ImmutableSet.of("date", "CAD", "HKD", "ISK", "PHP", "DKK", "HUF", "文", "somekey", "something", "nestedkey", "something2");

    assertEquals(expectedFieldNames, actualFieldNames);
  }

  @Test
  void testGetCatalogDiff() throws IOException {
    final JsonNode node1 = Jsons.deserialize(MoreResources.readResource("valid_schema.json"));
    final JsonNode node2 = Jsons.deserialize(MoreResources.readResource("valid_schema2.json"));
    final AirbyteCatalog catalog1 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName("users").withJsonSchema(node1),
        new AirbyteStream().withName("accounts").withJsonSchema(Jsons.emptyObject())));
    final AirbyteCatalog catalog2 = new AirbyteCatalog().withStreams(List.of(
        new AirbyteStream().withName("users").withJsonSchema(node2),
        new AirbyteStream().withName("sales").withJsonSchema(Jsons.emptyObject())));

    final Set<StreamTransform> actualDiff = CatalogHelpers.getCatalogDiff(catalog1, catalog2);
    final Set<StreamTransform> expectedDiff = Set.of(
        StreamTransform.createAddStreamTransform(new StreamDescriptor().withName("sales")),
        StreamTransform.createRemoveStreamTransform(new StreamDescriptor().withName("accounts")),
        StreamTransform.createUpdateStreamTransform(new UpdateStreamTransform(new StreamDescriptor().withName("users"), Set.of(
            FieldTransform.createAddFieldTransform(List.of("COD"), List.of(JsonSchemaType.NULL, JsonSchemaType.STRING)),
            FieldTransform.createRemoveFieldTransform(List.of("something2"), List.of()), // maps to unknown in the UI.
            FieldTransform.createRemoveFieldTransform(List.of("HKD"), List.of(JsonSchemaType.NULL, JsonSchemaType.NUMBER)),
            FieldTransform.createUpdateFieldTransform(new UpdateFieldTransform(
                List.of("CAD"),
                List.of(JsonSchemaType.NULL, JsonSchemaType.NUMBER),
                List.of(JsonSchemaType.NULL, JsonSchemaType.STRING)))))));
    assertEquals(expectedDiff, actualDiff);
  }

}
