package org.hypergraphql.query.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.hypergraphql.config.schema.HGQLVocabulary;
import org.hypergraphql.config.schema.QueryFieldConfig;
import org.hypergraphql.datafetching.services.SPARQLEndpointService;
import org.hypergraphql.datamodel.HGQLSchema;

public class SPARQLServiceConverter {

    private static final String RDF_TYPE_URI = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
    private static final String NAME = "name";
    private static final String URIS = "uris";
    private static final String NODE_ID = "nodeId";
    private static final String LANG = "lang";
    private static final String FIELDS = "fields";
    private static final String ARGS = "args";
    private static final String TARGET_NAME = "targetName";
    private static final String PARENT_ID = "parentId";
    private static final String LIMIT = "limit";
    private static final String OFFSET = "offset";

    private final HGQLSchema schema;

    public SPARQLServiceConverter(final HGQLSchema schema) {
        this.schema = schema;
    }

    private String optionalClause(final String sparqlPattern) {
        return " OPTIONAL { " + sparqlPattern + " } ";
    }

    private String selectSubqueryClause(final String id,
                                        final String sparqlPattern,
                                        final String limitOffset) {
        return "{ SELECT " + toVar(id) + " WHERE { " + sparqlPattern + " } " + limitOffset + " } ";
    }

    private String selectQueryClause(final String where, final String graphID) {
        return  "SELECT * WHERE { " + graphClause(graphID, where) + " } ";
    }

    private String graphClause(final String graphID, final String where) {
        if (StringUtils.isEmpty(graphID)) {
            return where;
        } else {
            return "GRAPH <" + graphID + "> { " + where + " } ";
        }
    }

    private String valuesClause(final String id, final Collection<String> input) {
        final String var = toVar(id);
        final Set<String> uris = new HashSet<>();
        input.forEach(uri -> uris.add(uriToResource(uri)));

        final String urisConcat = String.join(" ", uris);

        return  "VALUES " + var + " { " + urisConcat + " } ";
    }

    private String filterClause(final String id, final Set<String> input) {

        final String var = toVar(id);
        final Set<String> uris = new HashSet<>();
        input.forEach(uri -> uris.add(uriToResource(uri)));

        final String urisConcat = String.join(" , ", uris);

        return "FILTER ( " + var + " IN ( " + urisConcat + " ) )";
    }

    private String limitOffsetClause(final JsonNode jsonQuery) {
        final JsonNode args = jsonQuery.get(ARGS);
        String limit = "";
        String offset = "";
        if (args != null) {
            if (args.has(LIMIT)) {
                limit = limitClause(args.get(LIMIT).asInt());
            }
            if (args.has(OFFSET)) {
                offset = offsetClause(args.get(OFFSET).asInt());
            }
        }
        return limit + offset;
    }

    private String limitClause(final int limit) {
        return "LIMIT " + limit + " ";
    }

    private String offsetClause(final int limit) {
        return "OFFSET " + limit + " ";
    }

    private String uriToResource(final String uri) {
        return "<" + uri + ">";
    }

    private String toVar(final String id) {
        return "?" + id;
    }

    private String toTriple(final String subject,
                            final String predicate,
                            final String object) {
        return subject + " " + predicate + " " + object + " .";
    }

    private String langFilterClause(final JsonNode field) {
        final String filterPattern = "FILTER (lang(%s) = \"%s\") . "; // TODO - extract constant
        final String nodeVar = toVar(field.get(NODE_ID).asText());
        final JsonNode args = field.get(ARGS);
        return (args.has(LANG)) ? String.format(filterPattern, nodeVar, args.get(LANG).asText()) : "";
    }

    private String fieldPattern(final String parentId,
                                final String nodeId,
                                final String predicateURI,
                                final String typeURI) {
        final String predicateTriple = (parentId.isEmpty()) ? "" : toTriple(toVar(parentId), uriToResource(predicateURI), toVar(nodeId));
        final String typeTriple = (typeURI.isEmpty()) ? "" : toTriple(toVar(nodeId), RDF_TYPE_URI, uriToResource(typeURI));
        return predicateTriple + typeTriple;
    }

    public String getSelectQuery(final JsonNode jsonQuery,
                                 final Collection<String> input,
                                 final String rootType) {

        final Map<String, QueryFieldConfig> queryFields = schema.getQueryFields();

        final var root = !jsonQuery.isArray() && queryFields.containsKey(jsonQuery.get(NAME).asText());

        if (root) {
            if (queryFields.get(jsonQuery.get(NAME).asText()).type().equals(HGQLVocabulary.HGQL_QUERY_GET_FIELD)) {
                return getSelectRoot_GET(jsonQuery);
            } else {
                return getSelectRoot_GET_BY_ID(jsonQuery);
            }
        } else {
            return getSelectNonRoot((ArrayNode) jsonQuery, input, rootType);
        }
    }

    private String getSelectRoot_GET_BY_ID(final JsonNode queryField) {

        final Iterator<JsonNode> urisIter = queryField.get(ARGS).get(URIS).elements();

        final Set<String> uris = new HashSet<>();

        urisIter.forEachRemaining(uri -> uris.add(uri.asText()));

        final var targetName = queryField.get(TARGET_NAME).asText();
        final var targetURI = schema.getTypes().get(targetName).getId();
        final var graphID = ((SPARQLEndpointService) schema.getQueryFields().get(queryField.get(NAME).asText()).service()).getGraph();
        final var nodeId = queryField.get(NODE_ID).asText();
        final var selectTriple = toTriple(toVar(nodeId), RDF_TYPE_URI, uriToResource(targetURI));
        final var valuesClause = valuesClause(nodeId, uris);
        final var filterClause = filterClause(nodeId, uris);
        final var subfields = queryField.get(FIELDS);
        final var subQuery = getSubQueries(subfields);

        return selectQueryClause(valuesClause + selectTriple + subQuery, graphID);
    }

    private String getSelectRoot_GET(final JsonNode queryField) {

        final var targetName = queryField.get(TARGET_NAME).asText();
        final var targetURI = schema.getTypes().get(targetName).getId();
        final var graphID = ((SPARQLEndpointService) schema.getQueryFields().get(queryField.get(NAME).asText()).service()).getGraph();
        final var nodeId = queryField.get(NODE_ID).asText();
        final var limitOffsetClause = limitOffsetClause(queryField);
        final var selectTriple = toTriple(toVar(nodeId), RDF_TYPE_URI, uriToResource(targetURI));
        final var rootSubquery = selectSubqueryClause(nodeId, selectTriple, limitOffsetClause);
        final var subfields = queryField.get(FIELDS);
        final var whereClause = getSubQueries(subfields);

        return selectQueryClause(rootSubquery + whereClause, graphID);
    }

    private String getSelectNonRoot(final ArrayNode jsonQuery,
                                    final Collection<String> input,
                                    final String rootType) {


        final var firstField = jsonQuery.elements().next();
        final var graphID = ((SPARQLEndpointService) schema.getTypes().get(rootType).getFields().get(firstField.get(NAME).asText()).getService()).getGraph();
        final var parentId = firstField.get(PARENT_ID).asText();
        final var valueString = valuesClause(parentId, input);
        final var whereClause = new StringBuilder();
        jsonQuery.elements().forEachRemaining(field -> whereClause.append(getFieldSubquery(field)));
        return selectQueryClause(valueString + (whereClause.toString()), graphID);
    }

    private String getFieldSubquery(final JsonNode fieldJson) {

        final String fieldName = fieldJson.get(NAME).asText();

        if (HGQLVocabulary.JSONLD.containsKey(fieldName)) {
            return "";
        }

        final var fieldURI = schema.getFields().get(fieldName).getId();
        final var targetName = fieldJson.get(TARGET_NAME).asText();
        final var parentId = fieldJson.get(PARENT_ID).asText();
        final var nodeId = fieldJson.get(NODE_ID).asText();
        final var langFilter = langFilterClause(fieldJson);
        final var typeURI = (schema.getTypes().containsKey(targetName)) ? schema.getTypes().get(targetName).getId() : "";
        final var fieldPattern = fieldPattern(parentId, nodeId, fieldURI, typeURI);
        final var subfields = fieldJson.get(FIELDS);
        final var remainder = getSubQueries(subfields);

        return optionalClause(fieldPattern + langFilter + remainder);
    }

    private String getSubQueries(final JsonNode subfields) {

        if (subfields.isNull()) {
            return "";
        }
        final StringBuilder whereClause = new StringBuilder();
        subfields.elements().forEachRemaining(field -> whereClause.append(getFieldSubquery(field)));
        return whereClause.toString();
    }
}
