package cn.synway.bigdata.midas;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import cn.synway.bigdata.midas.domain.MidasDataType;
import cn.synway.bigdata.midas.except.MidasUnknownException;
import cn.synway.bigdata.midas.settings.MidasConnectionSettings;
import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.util.MidasHttpClientBuilder;
import cn.synway.bigdata.midas.util.LogProxy;
import cn.synway.bigdata.midas.util.guava.StreamUtils;


public class MidasConnectionImpl implements MidasConnection {

	private static final int DEFAULT_RESULTSET_TYPE = ResultSet.TYPE_FORWARD_ONLY;

    private static final Logger log = LoggerFactory.getLogger(MidasConnectionImpl.class);

    private final CloseableHttpClient httpclient;

    private final MidasProperties properties;

    private String url;

    private boolean closed = false;

    private TimeZone timezone;
    private volatile String serverVersion;

    public MidasConnectionImpl(String url) {
        this(url, new MidasProperties());
    }

    public MidasConnectionImpl(String url, MidasProperties properties) {
        this.url = url;
        try {
            this.properties = MidasJdbcUrlParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        MidasHttpClientBuilder clientBuilder = new MidasHttpClientBuilder(this.properties);
        log.debug("Create a new connection to {}", url);
        try {
            httpclient = clientBuilder.buildClient();
        }catch (Exception e) {
            throw  new IllegalStateException("cannot initialize http client", e);
        }
        initTimeZone(this.properties);
    }

    private void initTimeZone(MidasProperties properties) {
        if (properties.isUseServerTimeZone() && !Strings.isNullOrEmpty(properties.getUseTimeZone())) {
            throw new IllegalArgumentException(String.format("only one of %s or %s must be enabled", MidasConnectionSettings.USE_SERVER_TIME_ZONE.getKey(), MidasConnectionSettings.USE_TIME_ZONE.getKey()));
        }
        if (!properties.isUseServerTimeZone() && Strings.isNullOrEmpty(properties.getUseTimeZone())) {
            throw new IllegalArgumentException(String.format("one of %s or %s must be enabled", MidasConnectionSettings.USE_SERVER_TIME_ZONE.getKey(), MidasConnectionSettings.USE_TIME_ZONE.getKey()));
        }
        if (properties.isUseServerTimeZone()) {
            ResultSet rs = null;
            try {
                timezone = TimeZone.getTimeZone("UTC"); // just for next query
                rs = createStatement().executeQuery("select timezone()");
                rs.next();
                String timeZoneName = rs.getString(1);
                timezone = TimeZone.getTimeZone(timeZoneName);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                StreamUtils.close(rs);
            }
        } else if (!Strings.isNullOrEmpty(properties.getUseTimeZone())) {
            timezone = TimeZone.getTimeZone(properties.getUseTimeZone());
        }
    }

    @Override
    public MidasStatement createStatement() throws SQLException {
        return createStatement(DEFAULT_RESULTSET_TYPE);
    }

    public MidasStatement createStatement(int resultSetType) throws SQLException {
        return LogProxy.wrap(
            MidasStatement.class,
            new MidasStatementImpl(
                httpclient,
                this,
                properties,
                resultSetType));
    }

    @Deprecated
    @Override
    public MidasStatement createMidasStatement() throws SQLException {
        return createStatement();
    }

    @Override
    public TimeZone getTimeZone() {
        return timezone;
    }

    private MidasStatement createMidasStatement(CloseableHttpClient httpClient) throws SQLException {
        return LogProxy.wrap(
            MidasStatement.class,
            new MidasStatementImpl(
                httpClient,
                this,
                properties,
                DEFAULT_RESULTSET_TYPE));
    }

    public PreparedStatement createPreparedStatement(String sql, int resultSetType) throws SQLException {
        return LogProxy.wrap(
            PreparedStatement.class,
            new MidasPreparedStatementImpl(
                httpclient,
                this,
                properties,
                sql,
                getTimeZone(),
                resultSetType));
    }

    public MidasPreparedStatement createMidasPreparedStatement(String sql, int resultSetType) throws SQLException {
        return LogProxy.wrap(
            MidasPreparedStatement.class,
            new MidasPreparedStatementImpl(
                httpclient,
                this,
                properties,
                sql,
                getTimeZone(),
                resultSetType));
    }


    @Override
    public MidasStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * lazily calculates and returns server version
     * @return server version string
     * @throws SQLException if something has gone wrong
     */
    @Override
    public String getServerVersion() throws SQLException {
        if (serverVersion == null) {
            ResultSet rs = createStatement().executeQuery("select version()");
            rs.next();
            serverVersion = rs.getString(1);
            rs.close();
        }
        return serverVersion;
    }

    @Override
    public MidasStatement createStatement(int resultSetType, int resultSetConcurrency,
                                               int resultSetHoldability) throws SQLException {
        if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY
            || resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException();
        }
        return createStatement(resultSetType);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return createPreparedStatement(sql, DEFAULT_RESULTSET_TYPE);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        try {
            httpclient.close();
            closed = true;
        } catch (IOException e) {
            throw new MidasUnknownException("HTTP client close exception", e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return LogProxy.wrap(DatabaseMetaData.class, new MidasDatabaseMetadata(url, this));
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        properties.setDatabase(catalog);
        URI old = URI.create(url.substring(MidasJdbcUrlParser.JDBC_PREFIX.length()));
        try {
            url = MidasJdbcUrlParser.JDBC_PREFIX +
                    new URI(old.getScheme(), old.getUserInfo(), old.getHost(), old.getPort(),
                            "/" + catalog, old.getQuery(), old.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        return properties.getDatabase();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return createPreparedStatement(sql, resultSetType);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createPreparedStatement(sql, resultSetType);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout value mustn't be less 0");
        }

        if (isClosed()) {
            return false;
        }

        boolean isAnotherHttpClient = false;
        CloseableHttpClient closeableHttpClient = null;
        try {
            if (timeout == 0) {
                closeableHttpClient = this.httpclient;
            } else {
                MidasProperties properties = new MidasProperties(this.properties);
                int timeoutMs = (int) TimeUnit.SECONDS.toMillis(timeout);
                properties.setConnectionTimeout(timeoutMs);
                properties.setMaxExecutionTime(timeout);
                properties.setSocketTimeout(timeoutMs);
                closeableHttpClient = new MidasHttpClientBuilder(properties).buildClient();
                isAnotherHttpClient = true;
            }

            Statement statement = createMidasStatement(closeableHttpClient);
            statement.execute("SELECT 1");
            statement.close();
            return true;
        } catch (Exception e) {
            boolean isFailOnConnectionTimeout =
                    e instanceof ConnectTimeoutException
                            || e.getCause() instanceof ConnectTimeoutException;

            if (!isFailOnConnectionTimeout) {
                log.warn("Something had happened while validating a connection", e);
            }

            return false;
        } finally {
            if (isAnotherHttpClient) {
                try {
                    closeableHttpClient.close();
                } catch (IOException e) {
                    log.warn("Can't close a http client", e);
                }
            }
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return new MidasArray(
            MidasDataType.resolveDefaultArrayDataType(typeName),
            elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
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

    @Override
    public void setSchema(String schema) throws SQLException {
        properties.setDatabase(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return properties.getDatabase();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    void cleanConnections() {
        httpclient.getConnectionManager().closeExpiredConnections();
        httpclient.getConnectionManager().closeIdleConnections(2 * properties.getSocketTimeout(), TimeUnit.MILLISECONDS);
    }

    String getUrl() {
        return url;
    }

    MidasProperties getProperties() {
        return properties;
    }
}
