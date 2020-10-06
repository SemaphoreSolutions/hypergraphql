package org.hypergraphql.exception;

public class HGQLConfigurationException extends IllegalArgumentException {

    private static final String MESSAGE_PREFIX = "Unable to perform schema wiring: ";

    public HGQLConfigurationException(final String message) {
        super(MESSAGE_PREFIX + message);
    }

    public HGQLConfigurationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
