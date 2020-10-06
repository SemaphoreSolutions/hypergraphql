package org.hypergraphql.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.fuseki.embedded.FusekiServer;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Selector;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.rdf.model.impl.SelectorImpl;
import org.hypergraphql.Controller;
import org.hypergraphql.config.system.HGQLConfig;
import org.hypergraphql.services.HGQLConfigService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class SystemTest {

    private HGQLConfigService configService;
    private FusekiServer server;
    private Controller externalController;
    private Controller controller;
    private Model expectedModel;

    @AfterEach
    void tearDown() {
        externalController.stop();
        controller.stop();
        server.stop();
    }

    @BeforeEach
    void setUp() {

        Model mainModel = readModelFromFile("test_services/dbpedia.ttl");
        Model citiesModel = readModelFromFile("test_services/cities.ttl");

        expectedModel = ModelFactory.createDefaultModel()
                                    .add(mainModel)
                                    .add(citiesModel);

        Dataset ds = DatasetFactory.createTxnMem();
        ds.setDefaultModel(citiesModel);
        server = FusekiServer.create()
                .add("/ds", ds)
                .build()
                .start();

        HGQLConfig externalConfig = fromClasspathConfig("test_services/externalconfig.json");

        externalController = new Controller();
        externalController.start(externalConfig);

        HGQLConfig config = fromClasspathConfig("test_services/mainconfig.json");

        controller = new Controller();
        controller.start(config);
    }

    @Test
    void integration_test() {

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode bodyParam = mapper.createObjectNode();

        bodyParam.put("query", buildQuery());

        Model returnedModel = ModelFactory.createDefaultModel();

        try {
            HttpResponse<InputStream> response = Unirest.post("http://localhost:8080/graphql")
                    .header("Accept", "application/rdf+xml")
                    .body(bodyParam.toString())
                    .asBinary();

            returnedModel.read(response.getBody(), "RDF/XML");

        } catch (UnirestException e) {
            e.printStackTrace();
        }

        Resource res = ResourceFactory.createResource("http://hypergraphql.org/query");
        Selector sel = new SelectorImpl(res, null, (Object) null);
        StmtIterator iterator = returnedModel.listStatements(sel);
        Set<Statement> statements = new HashSet<>();
        while (iterator.hasNext()) {
            statements.add(iterator.nextStatement());
        }

        for (Statement statement : statements) {
            returnedModel.remove(statement);
        }

        compareModels(expectedModel, returnedModel);
        StmtIterator iterator2 = expectedModel.listStatements();
        while (iterator2.hasNext()) {
            assertTrue(returnedModel.contains(iterator2.next()));
        }

        assertTrue(expectedModel.isIsomorphicWith(returnedModel));
    }

    private HGQLConfig fromClasspathConfig(final String configPath) {

        if (configService == null) {
            configService = new HGQLConfigService();
        }

        final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configPath);
        return configService.loadHGQLConfig(configPath, inputStream, true);
    }

    private String buildQuery() {
        return "{\n"
                + "  Person_GET {\n"
                + "    _id\n"
                + "    label\n"
                + "    name\n"
                + "    birthPlace {\n"
                + "      _id\n"
                + "      label\n"
                + "    }\n"
                + "    \n"
                + "  }\n"
                + "  City_GET {\n"
                + "    _id\n"
                + "    label}\n"
                + "}";
    }

    private void compareModels(final Model expected, final Model actual) {

        log.info("Expected has {} statements", expected.size());
        log.info("Actual has {} statements", actual.size());

        final Model aEDiff = actual.difference(expected);
        log.info("Actual-Expected difference is {} triples", aEDiff.size());
        final Model eADiff = expected.difference(actual);
        log.info("Expected-Actual difference is {} triples", eADiff.size());

        assertTrue(expected.containsAll(actual));
        assertTrue(actual.containsAll(expected)); // 3 statements missing
    }

    private Model readModelFromFile(final String path) {
        final Model model = ModelFactory.createDefaultModel();
        final URL url = getClass().getClassLoader().getResource(path);
        if (url != null) {
            model.read(url.toString(), "TTL");
        }
        return model;
    }
}
