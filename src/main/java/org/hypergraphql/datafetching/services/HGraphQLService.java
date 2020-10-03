package org.hypergraphql.datafetching.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.hypergraphql.config.system.ServiceConfig;
import org.hypergraphql.datafetching.TreeExecutionResult;
import org.hypergraphql.datamodel.HGQLSchema;
import org.hypergraphql.query.converters.HGraphQLConverter;

@Slf4j
public class HGraphQLService extends Service {

    private String url;
    private String user;
    private String password;

    @Override
    public TreeExecutionResult executeQuery(
            final JsonNode query,
            final Collection<String> input,
            final Collection<String> markers,
            final String rootType,
            final HGQLSchema schema) {

        final Model model;
        final Map<String, Collection<String>> resultSet;
        final var graphQlQuery = HGraphQLConverter.convertToHGraphQL(schema, query, input, rootType);
        model = getModelFromRemote(graphQlQuery);

        resultSet = getResultset(model, query, input, markers, schema);

        final var treeExecutionResult = new TreeExecutionResult();
        treeExecutionResult.setResultSet(resultSet);
        treeExecutionResult.setModel(model);

        return treeExecutionResult;
    }

    Model getModelFromRemote(final String graphQlQuery) {

        final var mapper = new ObjectMapper();

        final var bodyParam = mapper.createObjectNode();

        bodyParam.put("query", graphQlQuery);

        final var model = ModelFactory.createDefaultModel();

        log.debug("\n" + url);
        log.debug("\n" + graphQlQuery);

        try {

            final HttpResponse<InputStream> response = Unirest.post(url)
                    .header("Accept", "application/rdf+xml")
                    .body(bodyParam.toString())
                    .asBinary();

            model.read(response.getBody(), "RDF/XML");

        } catch (UnirestException e) {
            e.printStackTrace();
        }

        return model;
    }

    @Override
    public void setParameters(ServiceConfig serviceConfig) {

        setId(serviceConfig.getId());
        this.url = serviceConfig.getUrl();
        this.user = serviceConfig.getUser();
        this.password = serviceConfig.getPassword();

    }
}

