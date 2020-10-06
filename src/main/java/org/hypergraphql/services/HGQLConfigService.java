package org.hypergraphql.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.hypergraphql.config.system.HGQLConfig;
import org.hypergraphql.datamodel.HGQLSchemaWiring;
import org.hypergraphql.exception.HGQLConfigurationException;
import org.hypergraphql.util.PathUtils;

// TODO - This needs better test coverage
@Slf4j
public class HGQLConfigService {

    private static final String S3_REGEX = "(?i)^https?://s3.*\\.amazonaws\\.com/.*";
    private static final String NORMAL_URL_REGEX = "(?i)^https?://.*";

    private final S3Service s3Service = new S3Service();

    public HGQLConfig loadHGQLConfig(final String hgqlConfigPath,
                                     final InputStream inputStream,
                                     final boolean classpath) {
        return loadHGQLConfig(hgqlConfigPath, inputStream, null, null, classpath);
    }

    HGQLConfig loadHGQLConfig(final String hgqlConfigPath,
                              final InputStream inputStream,
                              final String username,
                              final String password,
                              boolean classpath) {

        final var mapper = new ObjectMapper();

        try {

            final var config = mapper.readValue(inputStream, HGQLConfig.class);
            final var schemaParser = new SchemaParser();

            final var fullSchemaPath = extractFullSchemaPath(hgqlConfigPath, config.getSchemaFile());

            log.debug("Schema config path: " + fullSchemaPath);

            final var reader = selectAppropriateReader(fullSchemaPath, username, password, classpath);
            final var registry =
                    schemaParser.parse(reader);

            final var wiring = new HGQLSchemaWiring(registry, config.getName(), config.getServiceConfigs());
            config.setGraphQLSchema(wiring.getSchema());
            config.setHgqlSchema(wiring.getHgqlSchema());
            return config;

        } catch (IOException | URISyntaxException e) {
            throw new HGQLConfigurationException("Error reading from configuration file", e);
        }
    }

    private Reader selectAppropriateReader(final String schemaPath,
                                           final String username,
                                           final String password,
                                           final boolean classpath)
            throws IOException, URISyntaxException {

        if (schemaPath.matches(S3_REGEX)) {

            log.debug("S3 schema");
            // create S3 bucket request, etc.
            return getReaderForS3(schemaPath, username, password);

        } else if (schemaPath.matches(NORMAL_URL_REGEX)) {
            log.info("HTTP/S schema");
            return getReaderForUrl(schemaPath, username, password);
        } else if (schemaPath.contains(".jar!") || classpath) {
            log.debug("Class path schema");
            // classpath
            return getReaderForClasspath(schemaPath);
        } else {
            log.debug("Filesystem schema");
            // file
            return new InputStreamReader(new FileInputStream(schemaPath), StandardCharsets.UTF_8);
        }
    }

    private Reader getReaderForUrl(final String schemaPath,
                                   final String username,
                                   final String password) {

        final GetRequest getRequest;
        if (username == null && password == null) {
            getRequest = Unirest.get(schemaPath);
        } else {
            getRequest = Unirest.get(schemaPath).basicAuth(username, password);
        }

        try {
            final var body = getRequest.asString().getBody();
            return new StringReader(body);
        } catch (UnirestException e) {
            throw new HGQLConfigurationException("Unable to load configuration", e);
        }
    }

    private Reader getReaderForS3(final String schemaPath,
                                  final String username,
                                  final String password)
            throws URISyntaxException {

        final var uri = new URI(schemaPath);
        return new InputStreamReader(s3Service.openS3Stream(uri, username, password), StandardCharsets.UTF_8);
    }

    private String extractFullSchemaPath(final String hgqlConfigPath, final String schemaPath) {

        log.debug("HGQL config path: {}, schema path: {}", hgqlConfigPath, schemaPath);
        final var configPath = FilenameUtils.getFullPath(hgqlConfigPath);
        if (StringUtils.isBlank(configPath)) {
            return schemaPath;
        } else {
            final var abs = PathUtils.makeAbsolute(configPath, schemaPath);
            log.debug("Absolute path: {}", abs);
            return PathUtils.makeAbsolute(configPath, schemaPath);
        }
    }

    private Reader getReaderForClasspath(final String schemaPath) {

        log.debug("Obtaining reader for: {}", schemaPath);

        final var fn = schemaPath.contains("!")
                ? schemaPath.substring(schemaPath.lastIndexOf("!") + 1)
                : schemaPath;
        final var filename = fn.startsWith("/") ? fn.substring(fn.indexOf("/") + 1) : fn;
        log.debug("For filename: {}", filename);
        return new InputStreamReader(getClass().getClassLoader().getResourceAsStream(filename)); // TODO - address this
    }
}
