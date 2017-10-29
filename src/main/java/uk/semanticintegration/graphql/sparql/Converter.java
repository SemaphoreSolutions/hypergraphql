package uk.semanticintegration.graphql.sparql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.language.TypeDefinition;
import org.json.JSONArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szymon on 15/09/2017.
 * <p>
 * This class contains jsonRewrite methods between different query/response formats
 */
public class Converter {


    private JsonNode globalContext;
    private Map<String, String> JSONLD_VOC = new HashMap<String, String>() {{
        put("_context", "@context");
        put("_id", "@id");
        put("_value", "@value");
        put("_type", "@type");
        put("_language", "@language");
        put("_graph", "@graph");
    }}; //from graphql names to jsonld reserved names

    private LinkedList<QueryInQueue> queryQueue = new LinkedList<>();


    public Converter(Config config) {
        this.globalContext = config.context();
    }


    private class Traversal {
        Object data;
        Map<String, String> context;
    }

    private class QueryInQueue {
        JsonNode query;
        String rootNode;
        String childNode;

        QueryInQueue(JsonNode query) {
            this.query = query;
            this.childNode = "x";
        }

        QueryInQueue(JsonNode query, String rootNode, String childNode) {
            this.query = query;
            this.rootNode = rootNode;
            this.childNode = childNode;
        }
    }


    public Map<String,Object> jsonLDdata(String query, Map<String, Object> data) throws IOException {

        JsonNode jsonQuery = query2json(query);

        Map<String, Object> ldContext = new HashMap<>();
        Map<String, Object> output = new HashMap<>();

        jsonQuery.elements().forEachRemaining(el ->
                ldContext.put(el.get("name").asText(), "http://hypergraphql/"+el.get("name").asText()));

        Pattern namePtrn = Pattern.compile("\"name\":\"([^\"]*)\"");
        Matcher nameMtchr = namePtrn.matcher(jsonQuery.toString());

        while(nameMtchr.find())
        {
            String find = nameMtchr.group(1);
            if (!ldContext.containsKey(find)) {
                if (JSONLD_VOC.containsKey(find)) {
                    ldContext.put(find, JSONLD_VOC.get(find));
                } else {
                    if (globalContext.get("@predicates").has(find)) {
                        ldContext.put(find, globalContext.get("@predicates").get(find).get("@id"));
                    }
                }
            }
        }

        output.putAll(data);
        output.put("@context", ldContext);

        return output;
    }

    public List<String> graphql2sparql(String query) {

        JsonNode jsonQuery = query2json(query);

        for (JsonNode topQuery : jsonQuery) {
            QueryInQueue root = new QueryInQueue(topQuery);
            queryQueue.addLast(root);
        }

        List<String> output = new ArrayList<>();

        while (queryQueue.size() > 0) {

            QueryInQueue nextQuery = queryQueue.getFirst();
            queryQueue.removeFirst();

            try {
                String constructQuery = getConstructQuery(nextQuery);
                output.add(constructQuery);
            } catch (Exception e) {
                System.out.println(e.fillInStackTrace());
            }
        }

        return output;
    }

    public String getConstructQuery(QueryInQueue jsonQuery) {


        //this method will convert a given graphql query field into a SPARQL construct query
        // that will retrieve all relevant data in one go and put it into an in-memory jena store

        JsonNode root = jsonQuery.query;
        JsonNode args = root.get("args");
        String graphName = globalContext.get("@predicates").get(root.get("name").asText()).get("@namedGraph").asText();
        String graphId;
        if (!args.has("graph")) {
            graphId = globalContext.get("@namedGraphs").get(graphName).get("@id").asText();
        } else {
            graphId = args.get("graph").asText();
        }
        String endpointName = globalContext.get("@namedGraphs").get(graphName).get("@endpoint").asText();
        String endpointId;
        if (!args.has("endpoint")) {
            endpointId = globalContext.get("@endpoints").get(endpointName).get("@id").asText();
        } else {
            endpointId = args.get("endpoint").asText();
        }
        String uri_ref = String.format("<%s>", globalContext.get("@predicates").get(root.get("name").asText()).get("@id").asText());
        String parentNode = jsonQuery.rootNode;
        String selfNode = jsonQuery.childNode;
        String selfVar = "?" + selfNode;
        String parentVar = "?" + parentNode;
        String constructedTriple;
        String selectedTriple;
        String rootMatch = "";
        String nodeMark = "";
        String innerPattern;

        if (parentNode == null) {
            String limitParams = "";
            if (args.has("limit")) {
                limitParams = "LIMIT " + args.get("limit");
                if (args.has("offset")) {
                    limitParams = limitParams + " OFFSET " + args.get("offset");
                }
            }
            nodeMark = String.format("?%1$s <http://hgql/root> <http://hgql/node_%1$s> . ", selfNode );
            constructedTriple = String.format(" %1$s <http://hgql/root> %2$s . ", selfVar, uri_ref);
            selectedTriple = String.format(" %1$s a %2$s . ", selfVar, uri_ref);
            innerPattern = String.format(" { SELECT %s WHERE { %s } %s } ", selfVar, selectedTriple, limitParams);
        } else {
            rootMatch = String.format(" %s <http://hgql/root> <http://hgql/node_%s> . ", parentVar, parentNode);
            constructedTriple = String.format(" %s %s %s . ", parentVar, uri_ref, selfVar);
            innerPattern = constructedTriple;
        }

        String topConstructTemplate = "CONSTRUCT { " + nodeMark + " %1$s } WHERE { " + rootMatch + " SERVICE <%2$s> { %3$s } } ";

        String[] triplePatterns = getSubquery(root, selfNode, graphId, endpointId);

        constructedTriple = constructedTriple + triplePatterns[0];
        innerPattern = innerPattern + triplePatterns[1];
        String graphPattern = "GRAPH <%1$s> { %2$s }";

        if (!graphId.isEmpty()) {
            innerPattern = String.format(graphPattern, graphId, innerPattern);
        }

        return String.format(topConstructTemplate,
                constructedTriple,
                endpointId,
                innerPattern);
    }

    private String[] getSubquery(JsonNode node, String parentNode, String parentGraphId, String parentEndpointId) {

        String parentVar = "?" + parentNode;
        String constructPattern = "%1$s <%2$s> %3$s . ";
        String optionalPattern = "OPTIONAL { %1$s } ";
        String triplePattern = "%1$s <%2$s> %3$s %4$s. %5$s";
        String graphPattern = "GRAPH <%1$s> { %2$s }";

        String[] output = new String[2];

        JsonNode fields = node.get("fields");

        if (fields == null) {
            output[0] = "";
            output[1] = "";
        } else {

            int n = 0;

            List<String> childConstruct = new ArrayList<>();
            List<String> childOptional = new ArrayList<>();

            for (JsonNode field : fields) {

                if (!JSONLD_VOC.containsKey(field.get("name").asText())) {

                    JsonNode args = field.get("args");

                    String graphName = globalContext.get("@predicates").get(field.get("name").asText()).get("@namedGraph").asText();
                    String graphId;
                    if (!args.has("graph")) {
                        graphId = globalContext.get("@namedGraphs").get(graphName).get("@id").asText();
                    } else {
                        graphId = args.get("graph").asText();
                    }

                    String endpointName = globalContext.get("@namedGraphs").get(graphName).get("@endpoint").asText();
                    String endpointId;
                    if (!args.has("endpoint")) {
                        endpointId = globalContext.get("@endpoints").get(endpointName).get("@id").asText();
                    } else {
                        endpointId = args.get("endpoint").asText();
                    }

                    n++;

                    String childNode = parentNode + "_" + n;
                    String childVar = "?" + childNode;

                    if (!endpointId.equals(parentEndpointId)) {
                        //  System.out.println("Adding new query to queue");
                        QueryInQueue newQuery = new QueryInQueue(field, parentNode, childNode);
                        queryQueue.addLast(newQuery);

                        String nodeMark = String.format("?%1$s <http://hgql/root> <http://hgql/node_%1$s> .", parentNode);

                        childConstruct.add(nodeMark);

                        // childOptionalPattern = String.format(servicePattern, endpointId, childOptionalPattern);
                    } else {

                        String[] grandChildPatterns = getSubquery(field, childNode, graphId, endpointId);

                        String childConstructPattern = String.format(constructPattern,
                                parentVar,
                                globalContext.get("@predicates").get(field.get("name").asText()).get("@id").asText(),
                                childVar
                        );

                        String langFilter = "";

                        if (args.has("lang"))
                            langFilter = "FILTER (lang(" + childVar + ")=" + "\"" + args.get("lang").asText() + "\") ";

                        String childOptionalPattern = String.format(triplePattern,
                                parentVar,
                                globalContext.get("@predicates").get(field.get("name").asText()).get("@id").asText(),
                                childVar,
                                grandChildPatterns[1],
                                langFilter
                        );

                        if (!graphId.equals(""))
                            if (!graphId.equals(parentGraphId) || !endpointId.equals(parentEndpointId)) {
                                childOptionalPattern = String.format(graphPattern, graphId, childOptionalPattern);
                            }

                        childOptionalPattern = String.format(optionalPattern, childOptionalPattern);

                        childConstruct.add(childConstructPattern);
                        childConstruct.add(grandChildPatterns[0]);

                        childOptional.add(childOptionalPattern);
                    }
                }
            }

            output[0] = String.join(" ", childConstruct);
            output[1] = String.join(" ", childOptional);

        }
        return output;
    }

    public JsonNode query2json(String query) {


        query = query
                .replaceAll(",", " ")
                .replaceAll("\\s*:\\s*", ":")
                .replaceAll(",", " ")
                .replaceAll("\\{", " { ")
                .replaceAll("}", " } ")
                .replaceAll("\\(", " ( ")
                .replaceAll("\\)", " ) ")
                .replaceAll("\\s+", " ")
                .replaceAll("\\{", "<")
                .replaceAll("}", ">");

        Pattern namePtrn;
        Matcher nameMtchr;

        do {
            namePtrn = Pattern.compile("\\s(\\w+)\\s");
            nameMtchr = namePtrn.matcher(query);

            query = query.replaceAll("\\s(\\w+)\\s", " \"name\":\"$1\" ");

        } while (nameMtchr.find());

        do {
            namePtrn = Pattern.compile("\\s(\\w+):");
            nameMtchr = namePtrn.matcher(query);

            query = query.replaceAll("\\s(\\w+):", " \"$1\":");

        } while (nameMtchr.find());

        do {
            namePtrn = Pattern.compile("[^{](\"name\":\"\\w+\")(\\s(\\(\\s([^()]*)\\s\\)))?(\\s<([^<>]*)>)");
            nameMtchr = namePtrn.matcher(query);

            query = query
                    .replaceAll("(\"name\":\"\\w+\")\\s\\(\\s([^()]*)\\s\\)\\s<([^<>]*)>", "{$1, \"args\":{$2}, \"fields\":[$3]}")
                    .replaceAll("(\"name\":\"\\w+\")\\s<([^<>]*)>", "{$1, \"args\":{}, \"fields\":[$2]}");

        } while (nameMtchr.find());

        query = query
                .replaceAll("(\"name\":\"\\w+\")\\s\\(\\s([^()]*)\\s\\)", "{$1, \"args\":{$2}}")
                .replaceAll("(\"name\":\"\\w+\")\\s", "{$1, \"args\":{}} ");

        query = query
                .replaceAll("([^,])\\s\"", "$1, \"")
                .replaceAll("}\\s*\\{", "}, {")
                .replaceAll("<", "[")
                .replaceAll(">", "]");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode object = mapper.readTree(query);

            // System.out.println(object.toString()); //debug message

            return object;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public JsonNode definitionToJson(TypeDefinition type) {

        String typeData = type.toString();
        Pattern namePtrn = Pattern.compile("(\\w+)\\{");
        Matcher nameMtchr = namePtrn.matcher(typeData);

        while (nameMtchr.find()) {
            String find = nameMtchr.group(1);
            typeData = typeData.replace(find + "{", "{\'_type\':\'" + find + "\', ");
        }

        namePtrn = Pattern.compile("(\\w+)=");
        nameMtchr = namePtrn.matcher(typeData);

        while (nameMtchr.find()) {
            String find = nameMtchr.group(1);
            typeData = typeData.replace(" " + find + "=", "\'" + find + "\':");
        }

        typeData = typeData.replace("'", "\"");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode object = mapper.readTree(typeData);

            return object;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

}