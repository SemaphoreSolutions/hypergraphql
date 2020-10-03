package org.hypergraphql.query;

import graphql.language.Document;
import graphql.parser.Parser;
import graphql.schema.GraphQLSchema;
import graphql.validation.ValidationError;
import graphql.validation.Validator;
import java.util.ArrayList;
import java.util.List;

public class QueryValidator {

    private final GraphQLSchema schema;
    private final List<ValidationError> validationErrors;
    private final Validator validator;
    private final Parser parser;

    public QueryValidator(final GraphQLSchema schema) {

        this.schema = schema;
        this.validationErrors = new ArrayList<>();
        this.validator = new Validator();
        this.parser = new Parser();
    }

    public ValidatedQuery validateQuery(final String query) {

        final ValidatedQuery result = new ValidatedQuery();
        result.setErrors(validationErrors);

        final Document document;

        document = parser.parseDocument(query);
        result.setParsedQuery(document);

        validationErrors.addAll(validator.validateDocument(schema, document));
        result.setValid(validationErrors.size() == 0);
        return result;
    }
}
