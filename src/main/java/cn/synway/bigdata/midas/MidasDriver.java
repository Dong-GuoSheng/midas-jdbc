package cn.synway.bigdata.midas;

import com.google.common.collect.MapMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.synway.bigdata.midas.settings.MidasConnectionSettings;
import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.settings.MidasQueryParam;
import cn.synway.bigdata.midas.settings.DriverPropertyCreator;
import cn.synway.bigdata.midas.util.LogProxy;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 *
 * URL Format
 *
 * primitive for now
 *
 * jdbc:midas://host:port
 *
 * for example, jdbc:midas://localhost:8123
 *
 */
public class MidasDriver implements Driver {

    private static final Logger logger = LoggerFactory.getLogger(MidasDriver.class);

    private static final ConcurrentMap<MidasConnectionImpl, Boolean> connections = new MapMaker().weakKeys().makeMap();

    static {
        MidasDriver driver = new MidasDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        logger.info("Driver registered");
    }

    @Override
    public MidasConnection connect(String url, Properties info) throws SQLException {
        return connect(url, new MidasProperties(info));
    }

    public MidasConnection connect(String url, MidasProperties properties) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        logger.debug("Creating connection");
        MidasConnectionImpl connection = new MidasConnectionImpl(url, properties);
        registerConnection(connection);
        return LogProxy.wrap(MidasConnection.class, connection);
    }

    private void registerConnection(MidasConnectionImpl connection) {
        connections.put(connection, Boolean.TRUE);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(MidasJdbcUrlParser.JDBC_Midas_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties copy = new Properties(info);
        Properties properties;
        try {
            properties = MidasJdbcUrlParser.parse(url, copy).asProperties();
        } catch (Exception ex) {
            properties = copy;
            logger.error("could not parse url {}", url, ex);
        }
        List<DriverPropertyInfo> result = new ArrayList<DriverPropertyInfo>(MidasQueryParam.values().length
                + MidasConnectionSettings.values().length);
        result.addAll(dumpProperties(MidasQueryParam.values(), properties));
        result.addAll(dumpProperties(MidasConnectionSettings.values(), properties));
        return result.toArray(new DriverPropertyInfo[0]);
    }

    private List<DriverPropertyInfo> dumpProperties(DriverPropertyCreator[] creators, Properties info) {
        List<DriverPropertyInfo> result = new ArrayList<DriverPropertyInfo>(creators.length);
        for (DriverPropertyCreator creator : creators) {
            result.add(creator.createDriverPropertyInfo(info));
        }
        return result;
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Schedules connections cleaning at a rate. Turned off by default.
     * See https://hc.apache.org/httpcomponents-client-4.5.x/tutorial/html/connmgmt.html#d5e418
     *
     * @param rate period when checking would be performed
     * @param timeUnit time unit of rate
     */
    public void scheduleConnectionsCleaning(int rate, TimeUnit timeUnit){
        ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    for (MidasConnectionImpl connection : connections.keySet()) {
                        connection.cleanConnections();
                    }
                } catch (Exception e){
                    logger.error("error evicting connections: " + e);
                }
            }
        }, 0, rate, timeUnit);
    }

    static class ScheduledConnectionCleaner {
        static final ScheduledExecutorService INSTANCE = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());

        static class DaemonThreadFactory implements ThreadFactory {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setDaemon(true);
                return thread;
            }
        }
    }
}
