/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.protocol.models;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airbyte.commons.json.JsonSchemas;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.MoreLists;
import io.airbyte.protocol.models.transform_models.FieldTransform;
import io.airbyte.protocol.models.transform_models.StreamTransform;
import io.airbyte.protocol.models.transform_models.UpdateFieldTransform;
import io.airbyte.protocol.models.transform_models.UpdateStreamTransform;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Helper class for Catalog and Stream related operations. Generally only used in tests.
 */
public class CatalogHelpers {

  public static AirbyteCatalog createAirbyteCatalog(final String streamName, final Field... fields) {
    return new AirbyteCatalog().withStreams(Lists.newArrayList(createAirbyteStream(streamName, fields)));
  }

  public static AirbyteStream createAirbyteStream(final String streamName, final Field... fields) {
    // Namespace is null since not all sources set it.
    return createAirbyteStream(streamName, null, Arrays.asList(fields));
  }

  public static AirbyteStream createAirbyteStream(final String streamName, final String namespace, final Field... fields) {
    return createAirbyteStream(streamName, namespace, Arrays.asList(fields));
  }

  public static AirbyteStream createAirbyteStream(final String streamName, final String namespace, final List<Field> fields) {
    return new AirbyteStream().withName(streamName).withNamespace(namespace).withJsonSchema(fieldsToJsonSchema(fields));
  }

  public static ConfiguredAirbyteCatalog createConfiguredAirbyteCatalog(final String streamName, final String namespace, final Field... fields) {
    return new ConfiguredAirbyteCatalog().withStreams(Lists.newArrayList(createConfiguredAirbyteStream(streamName, namespace, fields)));
  }

  public static ConfiguredAirbyteCatalog createConfiguredAirbyteCatalog(final String streamName, final String namespace, final List<Field> fields) {
    return new ConfiguredAirbyteCatalog().withStreams(Lists.newArrayList(createConfiguredAirbyteStream(streamName, namespace, fields)));
  }

  public static ConfiguredAirbyteStream createConfiguredAirbyteStream(final String streamName, final String namespace, final Field... fields) {
    return createConfiguredAirbyteStream(streamName, namespace, Arrays.asList(fields));
  }

  public static ConfiguredAirbyteStream createConfiguredAirbyteStream(final String streamName, final String namespace, final List<Field> fields) {
    return new ConfiguredAirbyteStream()
        .withStream(new AirbyteStream().withName(streamName).withNamespace(namespace).withJsonSchema(fieldsToJsonSchema(fields)))
        .withSyncMode(SyncMode.FULL_REFRESH).withDestinationSyncMode(DestinationSyncMode.OVERWRITE);
  }

  /**
   * Convert a Catalog into a ConfiguredCatalog. This applies minimum default to the Catalog to make
   * it a valid ConfiguredCatalog.
   *
   * @param catalog - Catalog to be converted.
   * @return - ConfiguredCatalog based of off the input catalog.
   */
  public static ConfiguredAirbyteCatalog toDefaultConfiguredCatalog(final AirbyteCatalog catalog) {
    return new ConfiguredAirbyteCatalog()
        .withStreams(catalog.getStreams()
            .stream()
            .map(CatalogHelpers::toDefaultConfiguredStream)
            .collect(Collectors.toList()));
  }

  public static ConfiguredAirbyteStream toDefaultConfiguredStream(final AirbyteStream stream) {
    return new ConfiguredAirbyteStream()
        .withStream(stream)
        .withSyncMode(SyncMode.FULL_REFRESH)
        .withCursorField(new ArrayList<>())
        .withDestinationSyncMode(DestinationSyncMode.OVERWRITE)
        .withPrimaryKey(new ArrayList<>());
  }

  public static JsonNode fieldsToJsonSchema(final Field... fields) {
    return fieldsToJsonSchema(Arrays.asList(fields));
  }

  /**
   * Maps a list of fields into a JsonSchema object with names and types. This method will throw if it
   * receives multiple fields with the same name.
   *
   * @param fields fields to map to JsonSchema
   * @return JsonSchema representation of the fields.
   */
  public static JsonNode fieldsToJsonSchema(final List<Field> fields) {
    return Jsons.jsonNode(ImmutableMap.builder()
        .put("type", "object")
        .put("properties", fields
            .stream()
            .collect(Collectors.toMap(
                Field::getName,
                field -> {
                  if (isObjectWithSubFields(field)) {
                    return fieldsToJsonSchema(field.getSubFields());
                  } else {
                    return field.getType().getJsonSchemaTypeMap();
                  }
                })))
        .build());
  }

  /**
   * Gets the keys from the top-level properties object in the json schema.
   *
   * @param stream - airbyte stream
   * @return field names
   */
  @SuppressWarnings("unchecked")
  public static Set<String> getTopLevelFieldNames(final ConfiguredAirbyteStream stream) {
    // it is json, so the key has to be a string.
    final Map<String, Object> object = Jsons.object(stream.getStream().getJsonSchema().get("properties"), Map.class);
    return object.keySet();
  }

  /**
   * @param node any json node
   * @return a set of all keys for all objects within the node
   */
  @VisibleForTesting
  protected static Set<String> getAllFieldNames(final JsonNode node) {
    return getFullyQualifiedFieldNamesWithTypes(node)
        .stream()
        .map(Pair::getLeft)
        .map(MoreLists::last)
        .flatMap(Optional::stream)
        .collect(Collectors.toSet());
  }

  /**
   * @param node any json node
   * @return a set of all keys for all objects within the node
   */
  @VisibleForTesting
  protected static Set<Pair<List<String>, List<JsonSchemaType>>> getFullyQualifiedFieldNamesWithTypes(final JsonNode node) {
    return getFullyQualifiedFieldNamesWithTypes(node, new ArrayList<>());
  }

  protected static Set<Pair<List<String>, List<JsonSchemaType>>> getFullyQualifiedFieldNamesWithTypes(final JsonNode node,
                                                                                                      final List<String> pathToNode) {
    final Set<Pair<List<String>, List<JsonSchemaType>>> allFieldNames = new HashSet<>();

    if (node.has("properties")) {
      final JsonNode properties = node.get("properties");
      final Iterator<Map.Entry<String, JsonNode>> fieldNames = properties.fields();
      while (fieldNames.hasNext()) {
        final Map.Entry<String, JsonNode> entry = fieldNames.next();
        final String fieldName = entry.getKey();
        // todo (cgardens) build JsonSchemaType with airbyte types.
        final List<JsonSchemaType> fieldType = JsonSchemas.getType(entry.getValue())
            .stream()
            .map(jsonSchemaType -> JsonSchemaType.builder(JsonSchemaPrimitive.valueOf(jsonSchemaType.toUpperCase())).build())
            .toList();
        final List<String> pathWithThisField = new ArrayList<>(pathToNode);
        pathWithThisField.add(fieldName);
        allFieldNames.add(Pair.of(pathWithThisField, fieldType));
        final JsonNode fieldValue = properties.get(fieldName);
        if (fieldValue.isObject()) {
          allFieldNames.addAll(getFullyQualifiedFieldNamesWithTypes(fieldValue, pathWithThisField));
        }
      }
    }

    return allFieldNames;
  }

  private static boolean isObjectWithSubFields(final Field field) {
    return field.getType() == JsonSchemaType.OBJECT && field.getSubFields() != null && !field.getSubFields().isEmpty();
  }

  public static StreamDescriptor extractStreamDescriptor(final AirbyteStream airbyteStream) {
    return new StreamDescriptor().withName(airbyteStream.getName()).withNamespace(airbyteStream.getNamespace());
  }

  private static Map<StreamDescriptor, AirbyteStream> streamDescriptorToMap(final AirbyteCatalog catalog) {
    return catalog.getStreams()
        .stream()
        .collect(Collectors.toMap(CatalogHelpers::extractStreamDescriptor, s -> s));
  }

  /**
   * Returns difference between two provided catalogs.
   *
   * @param oldCatalog - old catalog
   * @param newCatalog - new catalog
   * @return difference between old and new catalogs
   */
  public static Set<StreamTransform> getCatalogDiff(final AirbyteCatalog oldCatalog, final AirbyteCatalog newCatalog) {
    final Set<StreamTransform> streamTransforms = new HashSet<>();

    final Map<StreamDescriptor, AirbyteStream> descriptorToStreamOld = streamDescriptorToMap(oldCatalog);
    final Map<StreamDescriptor, AirbyteStream> descriptorToStreamNew = streamDescriptorToMap(newCatalog);

    Sets.difference(descriptorToStreamOld.keySet(), descriptorToStreamNew.keySet())
        .forEach(descriptor -> streamTransforms.add(StreamTransform.createRemoveStreamTransform(descriptor)));
    Sets.difference(descriptorToStreamNew.keySet(), descriptorToStreamOld.keySet())
        .forEach(descriptor -> streamTransforms.add(StreamTransform.createAddStreamTransform(descriptor)));
    Sets.intersection(descriptorToStreamOld.keySet(), descriptorToStreamNew.keySet())
        .forEach(descriptor -> {
          final AirbyteStream streamOld = descriptorToStreamOld.get(descriptor);
          final AirbyteStream streamNew = descriptorToStreamNew.get(descriptor);
          if (!streamOld.equals(streamNew)) {
            streamTransforms.add(StreamTransform.createUpdateStreamTransform(getStreamDiff(descriptor, streamOld, streamNew)));
          }
        });

    return streamTransforms;
  }

  private static UpdateStreamTransform getStreamDiff(final StreamDescriptor descriptor,
                                                     final AirbyteStream streamOld,
                                                     final AirbyteStream streamNew) {
    final Set<FieldTransform> fieldTransforms = new HashSet<>();
    final Map<List<String>, List<JsonSchemaType>> fieldNameToTypeOld = getFullyQualifiedFieldNamesWithTypes(streamOld.getJsonSchema())
        .stream()
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    final Map<List<String>, List<JsonSchemaType>> fieldNameToTypeNew = getFullyQualifiedFieldNamesWithTypes(streamNew.getJsonSchema())
        .stream()
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    Sets.difference(fieldNameToTypeOld.keySet(), fieldNameToTypeNew.keySet())
        .forEach(fieldName -> fieldTransforms.add(FieldTransform.createRemoveFieldTransform(fieldName, fieldNameToTypeOld.get(fieldName))));
    Sets.difference(fieldNameToTypeNew.keySet(), fieldNameToTypeOld.keySet())
        .forEach(fieldName -> fieldTransforms.add(FieldTransform.createAddFieldTransform(fieldName, fieldNameToTypeNew.get(fieldName))));
    Sets.intersection(fieldNameToTypeOld.keySet(), fieldNameToTypeNew.keySet()).forEach(fieldName -> {
      final List<JsonSchemaType> oldType = fieldNameToTypeOld.get(fieldName);
      final List<JsonSchemaType> newType = fieldNameToTypeNew.get(fieldName);

      if (!oldType.equals(newType)) {
        fieldTransforms.add(FieldTransform.createUpdateFieldTransform(new UpdateFieldTransform(fieldName, oldType, newType)));
      }
    });
    return new UpdateStreamTransform(descriptor, fieldTransforms);
  }

}
