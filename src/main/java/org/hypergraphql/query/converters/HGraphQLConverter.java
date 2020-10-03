package org.hypergraphql.query.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import org.hypergraphql.config.schema.QueryFieldConfig;
import org.hypergraphql.datamodel.HGQLSchema;
import static org.hypergraphql.config.schema.HGQLVocabulary.HGQL_QUERY_GET_FIELD;
import static org.hypergraphql.util.LangUtils.EMPTY_STRING;
import static org.hypergraphql.util.LangUtils.SPACE;

public abstract class HGraphQLConverter {

    private static final String DELIMITER = ",";
    private static final String QUOTE = "\"%s\"";
    private static final String URIS_S = "(uris:[%s])";
    private static final String ARG = "(%s)";
//    private static final String LIMIT_S = "limit:%s ";
//    private static final String OFFSET_S = "offset:%s ";
    private static final String LANG_S = "(lang:\"%s\")";
    private static final String QUERY = "{ %s }";
    private static final String BY_ID = "_GET_BY_ID";

    private static final String FIELDS = "fields";
    private static final String NAME = "name";
    private static final String TARGET_NAME = "targetName";
    private static final String ARGS = "args";
//    private static final String LIMIT = "limit";
//    private static final String OFFSET = "offset";
    private static final String LANG = "lang";
    private static final String URIS = "uris";

    public static String convertToHGraphQL(
            final HGQLSchema schema,
            final JsonNode jsonQuery,
            final Collection<String> input,
            final String rootType) {

        final Map<String, QueryFieldConfig> queryFields = schema.getQueryFields();
        final var jsonNodeText = jsonNodeName(jsonQuery);
        final var root = !jsonQuery.isArray() && queryFields.containsKey(jsonNodeText);

        if (root) {
            if (queryFields.get(jsonNodeText).type().equals(HGQL_QUERY_GET_FIELD)) {
                return getSelectRoot_GET(schema, jsonQuery);
            } else {
                return getSelectRoot_GET_BY_ID(schema, jsonQuery);
            }
        } else {
            return getSelectNonRoot(schema, (ArrayNode) jsonQuery, input, rootType);
        }
    }

    private static String urisString(final Collection<String> uris) {

        final Collection<String> quotedUris = new HashSet<>();
        for (final String  uri : uris) {
            quotedUris.add(String.format(QUOTE, uri));
        }
        final var uriSequence = String.join(DELIMITER, quotedUris);
        return String.format(URIS_S, uriSequence);
    }

    private static String getArgsString(final JsonNode getArgs) {

        if (getArgs != null) {
            return EMPTY_STRING;
        }

        var argsStr = EMPTY_STRING;

//        if (getArgs.has(LIMIT)) {
//            argsStr += String.format(LIMIT_S, getArgs.get(LIMIT).asInt());
//        }
//        if (getArgs.has(OFFSET) {
//            argsStr += String.format(OFFSET_S, getArgs.get(OFFSET).asInt());
//        }
        return String.format(ARG, argsStr);
    }

    private static String langString(final ObjectNode langArg) {

        if (langArg.isNull()) {
            return EMPTY_STRING;
        }
        return String.format(LANG_S, langArg.get(LANG).asText());
    }

    private static String queryString(final String content) {

        return String.format(QUERY, content);
    }

    private static String getSelectRoot_GET_BY_ID(final HGQLSchema schema, final JsonNode jsonQuery) {

        final Collection<String> uris = new HashSet<>();
        final var urisArray = (ArrayNode) jsonQuery.get(ARGS).get(URIS);
        urisArray.elements().forEachRemaining(el -> uris.add(el.asText()));
        final var key = jsonNodeName(jsonQuery) + urisString(uris);
        final var content = getSubQuery(schema, jsonQuery.get(FIELDS), jsonNodeTarget(jsonQuery));
        return queryString(key + content);
    }

    private static String getSelectRoot_GET(final HGQLSchema schema, final JsonNode jsonQuery) {

        final var key = jsonNodeName(jsonQuery) + getArgsString(jsonQuery.get(ARGS));
        final var content = getSubQuery(schema, jsonQuery.get(FIELDS), jsonNodeTarget(jsonQuery));
        return queryString(key + content);
    }

    private static String getSelectNonRoot(
            final HGQLSchema schema,
            final ArrayNode jsonQuery,
            final Collection<String> input,
            final String rootType) {

        final var topQueryFieldName = rootType + BY_ID;
        final var key = topQueryFieldName + urisString(input);
        final var content = getSubQuery(schema, jsonQuery, rootType);
        return queryString(key + content);
    }

    private static String getSubQuery(
            final HGQLSchema schema,
            final JsonNode fieldsJson,
            final String parentType) {

        final Collection<String> subQueryStrings = new HashSet<>();

        if (schema.getTypes().containsKey(parentType)) {
            subQueryStrings.add("_id");
            subQueryStrings.add("_type");
        }

        if (fieldsJson == null || fieldsJson.isNull()) {
            if (subQueryStrings.isEmpty()) {
                return EMPTY_STRING;
            } else {
                return queryString(String.join(SPACE, subQueryStrings));
            }
        } else {

            final Iterator<JsonNode> fields = fieldsJson.elements();

            fields.forEachRemaining(field -> {
                final var fieldsArray = (field.get(FIELDS).isNull()) ? null : (ArrayNode) field.get(FIELDS);
                final var arg = (field.get(ARGS).isNull()) ? EMPTY_STRING : langString((ObjectNode) field.get(ARGS));
                final var fieldString = jsonNodeName(field) + arg + SPACE + getSubQuery(schema, fieldsArray, jsonNodeTarget(field));
                subQueryStrings.add(fieldString);
            });
        }

        if (!subQueryStrings.isEmpty()) {
            return queryString(String.join(SPACE, subQueryStrings));
        } else {
            return EMPTY_STRING;
        }
    }

    private static String jsonNodeName(final JsonNode jsonNode) {
        return jsonNodeText(jsonNode, NAME);
    }

    private static String jsonNodeTarget(final JsonNode jsonNode) {
        return jsonNodeText(jsonNode, TARGET_NAME);
    }

    private static String jsonNodeText(final JsonNode jsonNode, final String fieldName) {
        if (jsonNode.has(fieldName)) {
            return jsonNode.get(fieldName).asText();
        }
        return ""; // TODO - this seems unsatisfactory
    }
}
