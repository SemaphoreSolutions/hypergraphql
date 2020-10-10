package org.hypergraphql.datafetching.services;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.hypergraphql.config.schema.HGQLVocabulary;
import org.hypergraphql.config.system.ServiceConfig;
import org.hypergraphql.datafetching.SPARQLEndpointExecution;
import org.hypergraphql.datafetching.SPARQLExecutionResult;
import org.hypergraphql.datafetching.TreeExecutionResult;
import org.hypergraphql.datamodel.HGQLSchema;

public class SPARQLEndpointService extends SPARQLService {

    static final int VALUES_SIZE_LIMIT = 100;
    private static final int THREADPOOL_SIZE = 50;

    private String url;
    private String user;
    private String password;

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public TreeExecutionResult executeQuery(final JsonNode query,
                                            final Collection<String> input,
                                            final Collection<String> markers,
                                            final String rootType,
                                            final HGQLSchema schema) {


        final Map<String, Collection<String>> resultSet = new HashMap<>(); // TODO - dupe
        final var unionModel = ModelFactory.createDefaultModel();
        final Set<Future<SPARQLExecutionResult>> futureSPARQLResults = new HashSet<>();

        final List<String> inputList = getStrings(query, input, markers, rootType, schema, resultSet);

        do {

            final Collection<String> inputSubset = new HashSet<>(); // TODO - dupe
            int i = 0;
            while (i < VALUES_SIZE_LIMIT && !inputList.isEmpty()) {
                inputSubset.add(inputList.get(0));
                inputList.remove(0);
                i++;
            }
            final var executor = Executors.newFixedThreadPool(THREADPOOL_SIZE);
            final var execution = createExecution(query, inputSubset, markers, schema, rootType);
            futureSPARQLResults.add(executor.submit(execution));

        } while (inputList.size() > VALUES_SIZE_LIMIT);

        iterateFutureResults(futureSPARQLResults, unionModel, resultSet);

        final var treeExecutionResult = new TreeExecutionResult();
        treeExecutionResult.setResultSet(resultSet);
        treeExecutionResult.setModel(unionModel);

        return treeExecutionResult;
    }

    void iterateFutureResults(
            final Collection<Future<SPARQLExecutionResult>> futureSPARQLResults,
            final Model unionModel,
            final Map<String, Collection<String>> resultSet
    ) {

        for (Future<SPARQLExecutionResult> futureExecutionResult : futureSPARQLResults) {
            try {
                final var result = futureExecutionResult.get();
                unionModel.add(result.getModel());
                resultSet.putAll(result.getResultSet());
            } catch (InterruptedException
                    | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    List<String> getStrings(final JsonNode query,
                            final Collection<String> input,
                            final Collection<String> markers,
                            final String rootType,
                            final HGQLSchema schema,
                            final Map<String, Collection<String>> resultSet) {
        for (final String marker : markers) {
            resultSet.put(marker, new HashSet<>());
        }

        if (query.has("name")) {
            final var typeName = getQueryFieldAsText(query, "name");
            final var queryType = schema.getQueryFields().get(typeName).type();
            if ("Query".equals(rootType)
                    && queryType.equals(HGQLVocabulary.HGQL_QUERY_GET_BY_ID_FIELD)) {
                final Iterator<JsonNode> uris = query.get("args").get("uris").elements();
                while (uris.hasNext()) {
                    input.add(uris.next().asText());
                }
            }
            return new ArrayList<>(input);
        }
        return List.of();
    }

    @Override
    public void setParameters(final ServiceConfig serviceConfig) {

        super.setParameters(serviceConfig);

        setId(serviceConfig.getId());
        this.url = serviceConfig.getUrl();
        this.user = serviceConfig.getUser();
        setGraph(serviceConfig.getGraph());
        this.password = serviceConfig.getPassword();

    }

    private SPARQLEndpointExecution createExecution(
            final JsonNode query,
            final Collection<String> inputSubset, // TODO - this needs a better name
            final Collection<String> markers,
            final HGQLSchema hgqlSchema,
            final String rootType
    ) {
        return new SPARQLEndpointExecution(
                query,
                inputSubset,
                markers,
                this,
                hgqlSchema,
                rootType
        );
    }

    private String getQueryFieldAsText(final JsonNode query, final String fieldName) {

        if (query.has(fieldName)) {
            return query.get(fieldName).asText();
        }
        return "";
    }
}
