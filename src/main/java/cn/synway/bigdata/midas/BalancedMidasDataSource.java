package cn.synway.bigdata.midas;

import org.slf4j.LoggerFactory;
import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.util.apache.StringUtils;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.synway.bigdata.midas.MidasJdbcUrlParser.JDBC_Midas_PREFIX;

/**
 * <p> Database for Midas jdbc connections.
 * <p> It has list of database urls.
 * For every {@link #getConnection() getConnection} invocation, it returns connection to random host from the list.
 * Furthermore, this class has method {@link #scheduleActualization(int, TimeUnit) scheduleActualization}
 * which test hosts for availability. By default, this option is turned off.
 */
public class BalancedMidasDataSource implements DataSource {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BalancedMidasDataSource.class);
    private static final Pattern URL_TEMPLATE = Pattern.compile(JDBC_Midas_PREFIX + "" +
            "//([a-zA-Z0-9_:,.-]+)" +
            "(/[a-zA-Z0-9_]+" +
            "([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+)*)?" +
            ")?");

    private PrintWriter printWriter;
    private int loginTimeoutSeconds = 0;

    private final ThreadLocal<Random> randomThreadLocal = new ThreadLocal<Random>();
    private final List<String> allUrls;
    private volatile List<String> enabledUrls;

    private final MidasProperties properties;
    private final MidasDriver driver = new MidasDriver();

    /**
     * create Datasource for Midas JDBC connections
     *
     * @param url address for connection to the database
     *            must have the next format {@code jdbc:midas://<first-host>:<port>,<second-host>:<port>/<database>?param1=value1&param2=value2 }
     *            for example, {@code jdbc:midas://localhost:8123,localhost:8123/database?compress=1&decompress=2 }
     * @throws IllegalArgumentException if param have not correct format, or error happens when checking host availability
     */
    public BalancedMidasDataSource(final String url) {
        this(splitUrl(url), getFromUrl(url));
    }

    /**
     * create Datasource for Midas JDBC connections
     *
     * @param url        address for connection to the database
     * @param properties database properties
     * @see #BalancedMidasDataSource(String)
     */
    public BalancedMidasDataSource(final String url, Properties properties) {
        this(splitUrl(url), new MidasProperties(properties));
    }

    /**
     * create Datasource for Midas JDBC connections
     *
     * @param url        address for connection to the database
     * @param properties database properties
     * @see #BalancedMidasDataSource(String)
     */
    public BalancedMidasDataSource(final String url, MidasProperties properties) {
        this(splitUrl(url), properties.merge(getFromUrlWithoutDefault(url)));
    }

    private BalancedMidasDataSource(final List<String> urls) {
        this(urls, new MidasProperties());
    }

    private BalancedMidasDataSource(final List<String> urls, Properties info) {
        this(urls, new MidasProperties(info));
    }

    private BalancedMidasDataSource(final List<String> urls, MidasProperties properties) {
        if (urls.isEmpty()) {
            throw new IllegalArgumentException("Incorrect Midas jdbc url list. It must be not empty");
        }

        try {
            MidasProperties localProperties = MidasJdbcUrlParser.parse(urls.get(0), properties.asProperties());
            localProperties.setHost(null);
            localProperties.setPort(-1);

            this.properties = localProperties;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }


        List<String> allUrls = new ArrayList<String>(urls.size());
        for (final String url : urls) {
            try {
                if (driver.acceptsURL(url)) {
                    allUrls.add(url);
                } else {
                    log.error("that url is has not correct format: {}", url);
                }
            } catch (SQLException e) {
                throw new IllegalArgumentException("error while checking url: " + url, e);
            }
        }

        if (allUrls.isEmpty()) {
            throw new IllegalArgumentException("there are no correct urls");
        }

        this.allUrls = Collections.unmodifiableList(allUrls);
        this.enabledUrls = this.allUrls;
    }

    static List<String> splitUrl(final String url) {
        Matcher m = URL_TEMPLATE.matcher(url);
        if (!m.matches()) {
            throw new IllegalArgumentException("Incorrect url");
        }
        String database = m.group(2);
        if (database == null) {
            database = "";
        }
        String[] hosts = m.group(1).split(",");
        final List<String> result = new ArrayList<String>(hosts.length);
        for (final String host : hosts) {
            result.add(JDBC_Midas_PREFIX + "//" + host + database);
        }
        return result;
    }


    private boolean ping(final String url) {
        try {
            driver.connect(url, properties).createStatement().execute("SELECT 1");
            return true;
        } catch (Exception e) {
            log.debug("Unable to connect using {}", url, e);
            return false;
        }
    }

    /**
     * Checks if Midas on url is alive, if it isn't, disable url, else enable.
     *
     * @return number of avaliable Midas urls
     */
    public synchronized int actualize() {
        List<String> enabledUrls = new ArrayList<String>(allUrls.size());

        for (String url : allUrls) {
            log.debug("Pinging disabled url: {}", url);
            if (ping(url)) {
                log.debug("Url is alive now: {}", url);
                enabledUrls.add(url);
            } else {
                log.debug("Url is dead now: {}", url);
            }
        }

        this.enabledUrls = Collections.unmodifiableList(enabledUrls);
        return enabledUrls.size();
    }


    private String getAnyUrl() throws SQLException {
        List<String> localEnabledUrls = enabledUrls;
        if (localEnabledUrls.isEmpty()) {
            throw new SQLException("Unable to get connection: there are no enabled urls");
        }
        Random random = this.randomThreadLocal.get();
        if (random == null) {
            this.randomThreadLocal.set(new Random());
            random = this.randomThreadLocal.get();
        }

        int index = random.nextInt(localEnabledUrls.size());
        return localEnabledUrls.get(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MidasConnection getConnection() throws SQLException {
        return driver.connect(getAnyUrl(), properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MidasConnection getConnection(String username, String password) throws SQLException {
        return driver.connect(getAnyUrl(), properties.withCredentials(username, password));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLogWriter(PrintWriter printWriter) throws SQLException {
        this.printWriter = printWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
//        throw new SQLFeatureNotSupportedException();
        loginTimeoutSeconds = seconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    /**
     * {@inheritDoc}
     */
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * set time period of removing connections
     *
     * @param rate     value for time unit
     * @param timeUnit time unit for checking
     * @return this datasource with changed settings
     * @see MidasDriver#scheduleConnectionsCleaning
     */
    public BalancedMidasDataSource withConnectionsCleaning(int rate, TimeUnit timeUnit) {
        driver.scheduleConnectionsCleaning(rate, timeUnit);
        return this;
    }

    /**
     * set time period for checking availability connections
     *
     * @param delay    value for time unit
     * @param timeUnit time unit for checking
     * @return this datasource with changed settings
     */
    public BalancedMidasDataSource scheduleActualization(int delay, TimeUnit timeUnit) {
        MidasDriver.ScheduledConnectionCleaner.INSTANCE.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    actualize();
                } catch (Exception e) {
                    log.error("Unable to actualize urls", e);
                }
            }
        }, 0, delay, timeUnit);

        return this;
    }

    public List<String> getAllMidasUrls() {
        return allUrls;
    }

    public List<String> getEnabledMidasUrls() {
        return enabledUrls;
    }

    public List<String> getDisabledUrls() {
        List<String> enabledUrls = this.enabledUrls;
        if (!hasDisabledUrls()) {
            return Collections.emptyList();
        }
        List<String> disabledUrls = new ArrayList<String>(allUrls);
        disabledUrls.removeAll(enabledUrls);
        return disabledUrls;
    }

    public boolean hasDisabledUrls() {
        return allUrls.size() != enabledUrls.size();
    }

    public MidasProperties getProperties() {
        return properties;
    }

    private static MidasProperties getFromUrl(String url) {
        return new MidasProperties(getFromUrlWithoutDefault(url));
    }

    private static Properties getFromUrlWithoutDefault(String url) {
        if (StringUtils.isBlank(url))
            return new Properties();

        int index = url.indexOf("?");
        if (index == -1)
            return new Properties();

        return MidasJdbcUrlParser.parseUriQueryPart(url.substring(index + 1), new Properties());
    }
}
