package org.hypergraphql.datafetching;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import graphql.language.Field;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.hypergraphql.datamodel.HGQLSchema;

@Slf4j
public class ExecutionForest  {

    private final Collection<ExecutionTreeNode> forest;

    public ExecutionForest() {
        this.forest = new HashSet<>();
    }

    public Collection<ExecutionTreeNode> getForest() {
        return forest; // TODO - this should be a deep copy
    }

    public Model generateModel() {

        final var executor = Executors.newFixedThreadPool(10);
        final var model = ModelFactory.createDefaultModel();
        final Set<Future<Model>> futureModels = new HashSet<>();
        forest.forEach(node -> {
            final var fetchingExecution = new FetchingExecution(new HashSet<>(), node);
            futureModels.add(executor.submit(fetchingExecution));
        });

        futureModels.forEach(futureModel -> {
            try {
                model.add(futureModel.get());
            } catch (InterruptedException | ExecutionException e) {
                log.error("Problem generating model", e);
            }
        });
        return model;
    }

    public String toString() {
        return this.toString(0);
    }

    public String toString(int i) {

        final var result = new StringBuilder();
        forest.forEach(node -> result.append(node.toString(i)));
        return result.toString();
    }

    public Map<String, String> getFullLdContext() {

        final Map<String, String> result = new HashMap<>();
        forest.forEach(child -> result.putAll(child.getFullLdContext()));
        return result;
    }

    public boolean addExecutionTreeNode(final Field field, final String nodeId, final HGQLSchema schema) {
        return this.forest.add(new ExecutionTreeNode(field, nodeId, schema));
    }
}
