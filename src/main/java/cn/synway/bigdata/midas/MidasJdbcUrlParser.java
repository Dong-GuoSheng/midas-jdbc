package cn.synway.bigdata.midas;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.settings.MidasQueryParam;

public class MidasJdbcUrlParser {
    private static final Logger logger = LoggerFactory.getLogger(MidasJdbcUrlParser.class);
    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_Midas_PREFIX = JDBC_PREFIX + "Midas:";
    public static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_*\\-]+)");
    protected final static String DEFAULT_DATABASE = "default";

    private MidasJdbcUrlParser(){
    }

    public static MidasProperties parse(String jdbcUrl, Properties defaults) throws URISyntaxException
    {
        if (!jdbcUrl.startsWith(JDBC_Midas_PREFIX)) {
            throw new URISyntaxException(jdbcUrl, "'" + JDBC_Midas_PREFIX + "' prefix is mandatory");
        }
        return parseMidasUrl(jdbcUrl.substring(JDBC_PREFIX.length()), defaults);
    }

    private static MidasProperties parseMidasUrl(String uriString, Properties defaults)
            throws URISyntaxException
    {
        URI uri = new URI(uriString);
        Properties urlProperties = parseUriQueryPart(uri.getQuery(), defaults);
        MidasProperties props = new MidasProperties(urlProperties);
        props.setHost(uri.getHost());
        int port = uri.getPort();
        if (port == -1) {
            throw new IllegalArgumentException("port is missed or wrong");
        }
        props.setPort(port);
        String path = uri.getPath();
        String database;
        if (props.isUsePathAsDb()) {
            if (path == null || path.isEmpty() || path.equals("/")) {
                String defaultsDb = defaults.getProperty(MidasQueryParam.DATABASE.getKey());
                database = defaultsDb == null ? DEFAULT_DATABASE : defaultsDb;
            } else {
                Matcher m = DB_PATH_PATTERN.matcher(path);
                if (m.matches()) {
                    database = m.group(1);
                } else {
                    throw new URISyntaxException("wrong database name path: '" + path + "'", uriString);
                }
            }
            props.setDatabase(database);
        } else {
            if (props.getDatabase() == null || props.getDatabase().isEmpty()) {
                props.setDatabase(DEFAULT_DATABASE);
            }
            if (path == null || path.isEmpty()) {
                props.setPath("/");
            } else {
                props.setPath(path);
            }
        }
        return props;
    }

    static Properties parseUriQueryPart(String query, Properties defaults) {
        if (query == null) {
            return defaults;
        }
        Properties urlProps = new Properties(defaults);
        String queryKeyValues[] = query.split("&");
        for (String keyValue : queryKeyValues) {
            String keyValueTokens[] = keyValue.split("=");
            if (keyValueTokens.length == 2) {
                urlProps.put(keyValueTokens[0], keyValueTokens[1]);
            } else {
                logger.warn("don't know how to handle parameter pair: {}", keyValue);
            }
        }
        return urlProps;
    }
}
