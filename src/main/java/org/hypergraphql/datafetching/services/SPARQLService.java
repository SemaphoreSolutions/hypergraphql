package org.hypergraphql.datafetching.services;

import org.hypergraphql.config.system.ServiceConfig;

public abstract class SPARQLService extends Service { // TODO - this could do with a better name

    private String graph;

    public String getGraph() {
        return graph;
    }

    public void setGraph(final String graphName) {
        this.graph = graphName;
    }

    public void setParameters(final ServiceConfig serviceConfig) {

        setId(serviceConfig.getId());
        if (serviceConfig.getGraph() == null) {
            this.graph = "";
        } else {
            this.graph = serviceConfig.getGraph();
        }
    }
}
