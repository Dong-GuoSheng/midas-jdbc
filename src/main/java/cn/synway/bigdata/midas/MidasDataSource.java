package cn.synway.bigdata.midas;

import cn.synway.bigdata.midas.settings.MidasProperties;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class MidasDataSource implements DataSource {
    protected final MidasDriver driver = new MidasDriver();
    protected final String url;
    protected PrintWriter printWriter;
    protected int loginTimeoutSeconds = 0;
    private MidasProperties properties;

    public MidasDataSource(String url) {
        this(url, new MidasProperties());
    }

    public MidasDataSource(String url, Properties info) {
        this(url, new MidasProperties(info));
    }

    public MidasDataSource(String url, MidasProperties properties) {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect Midas jdbc url. It must be not null");
        }
        this.url = url;
        try {
            this.properties =  MidasJdbcUrlParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public MidasConnection getConnection() throws SQLException {
        return driver.connect(url, properties);
    }

    @Override
    public MidasConnection getConnection(String username, String password) throws SQLException {
        return driver.connect(url, properties.withCredentials(username, password));
    }

    public String getHost() {
        return properties.getHost();
    }

    public int getPort() {
        return properties.getPort();
    }

    public String getDatabase() {
        return properties.getDatabase();
    }

    public String getUrl() {
        return url;
    }

    public MidasProperties getProperties() {
        return properties;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeoutSeconds = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    /**
     * Schedules connections cleaning at a rate. Turned off by default.
     * See https://hc.apache.org/httpcomponents-client-4.5.x/tutorial/html/connmgmt.html#d5e418
     *
     * @param rate period when checking would be performed
     * @param timeUnit time unit of rate
     * @return current modified object
     */
    public MidasDataSource withConnectionsCleaning(int rate, TimeUnit timeUnit){
        driver.scheduleConnectionsCleaning(rate, timeUnit);
        return this;
    }
}
