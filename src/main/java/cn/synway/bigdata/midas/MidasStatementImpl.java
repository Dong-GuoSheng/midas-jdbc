package cn.synway.bigdata.midas;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import cn.synway.bigdata.midas.domain.MidasFormat;
import cn.synway.bigdata.midas.except.MidasException;
import cn.synway.bigdata.midas.except.MidasExceptionSpecifier;
import cn.synway.bigdata.jdbc.parser.MidasSqlParser;
import cn.synway.bigdata.midas.jdbc.parser.MidasSqlStatement;
import cn.synway.bigdata.midas.jdbc.parser.StatementType;
import cn.synway.bigdata.midas.response.MidasLZ4Stream;
import cn.synway.bigdata.midas.response.MidasResponse;
import cn.synway.bigdata.midas.response.MidasResponseSummary;
import cn.synway.bigdata.midas.response.MidasResultSet;
import cn.synway.bigdata.midas.response.MidasScrollableResultSet;
import cn.synway.bigdata.midas.response.FastByteArrayOutputStream;
import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.settings.MidasQueryParam;
import cn.synway.bigdata.midas.util.MidasHttpClientBuilder;
import cn.synway.bigdata.midas.util.MidasRowBinaryInputStream;
import cn.synway.bigdata.midas.util.MidasStreamCallback;
import cn.synway.bigdata.midas.util.Utils;
import cn.synway.bigdata.midas.util.guava.StreamUtils;

public class MidasStatementImpl extends ConfigurableApi<MidasStatement> implements MidasStatement {

    private static final Logger log = LoggerFactory.getLogger(MidasStatementImpl.class);

    private final CloseableHttpClient client;

    private final HttpClientContext httpContext;

    protected MidasProperties properties;

    private MidasConnection connection;

    private MidasResultSet currentResult;

    private MidasRowBinaryInputStream currentRowBinaryResult;

    private MidasResponseSummary currentSummary;

    private int currentUpdateCount = -1;

    private int queryTimeout;

    private boolean isQueryTimeoutSet = false;

    private int maxRows;

    private boolean closeOnCompletion;

    private final boolean isResultSetScrollable;

    private volatile String queryId;

    protected MidasSqlStatement parsedStmt;

    /**
     * Current database name may be changed by {@link java.sql.Connection#setCatalog(String)}
     * between creation of this object and query execution, but javadoc does not allow
     * {@code setCatalog} influence on already created statements.
     */
    private final String initialDatabase;

    @Deprecated
    private static final String[] selectKeywords = new String[]{"SELECT", "WITH", "SHOW", "DESC", "EXISTS", "EXPLAIN"};
    @Deprecated
    private static final String databaseKeyword = "CREATE DATABASE";

    @Deprecated
    protected void parseSingleStatement(String sql) throws SQLException {
        this.parsedStmt = null;
        MidasSqlStatement[] stmts = MidasSqlParser.parse(sql, properties);
        
        if (stmts.length == 1) {
            this.parsedStmt = stmts[0];
        } else {
            this.parsedStmt = new MidasSqlStatement(sql, StatementType.UNKNOWN);
            // throw new SQLException("Multiple statements are not supported.");
        }

        if (this.parsedStmt.isIdemponent()) {
            httpContext.setAttribute("is_idempotent", Boolean.TRUE);
        } else {
            httpContext.removeAttribute("is_idempotent");
        }
    }

    @Deprecated
    private void parseSingleStatement(String sql, MidasFormat preferredFormat) throws SQLException {
        parseSingleStatement(sql);

        if (parsedStmt.isQuery() && !parsedStmt.hasFormat()) {
            String format = preferredFormat.name();
            Map<String, Integer> positions = new HashMap<>();
            positions.putAll(parsedStmt.getPositions());
            positions.put(MidasSqlStatement.KEYWORD_FORMAT, sql.length());
            
            sql = new StringBuilder(parsedStmt.getSQL()).append("\nFORMAT ").append(format).append(';')
                        .toString();
            parsedStmt = new MidasSqlStatement(sql, parsedStmt.getStatementType(), 
                parsedStmt.getCluster(), parsedStmt.getDatabase(), parsedStmt.getTable(),
                format, parsedStmt.getOutfile(), parsedStmt.getParameters(), positions);
        }
    }

    public MidasStatementImpl(CloseableHttpClient client, MidasConnection connection,
                                   MidasProperties properties, int resultSetType) {
        super(null);
        this.client = client;
        this.httpContext = MidasHttpClientBuilder.createClientContext(properties);
        this.connection = connection;
        this.properties = properties == null ? new MidasProperties() : properties;
        this.initialDatabase = this.properties.getDatabase();
        this.isResultSetScrollable = (resultSetType != ResultSet.TYPE_FORWARD_ONLY);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return executeQuery(sql, null);
    }

    @Override
    public ResultSet executeQuery(String sql, Map<MidasQueryParam, String> additionalDBParams) throws SQLException {
        return executeQuery(sql, additionalDBParams, null);
    }

    @Override
    public ResultSet executeQuery(String sql, Map<MidasQueryParam, String> additionalDBParams, List<MidasExternalData> externalData) throws SQLException {
        return executeQuery(sql, additionalDBParams, externalData, null);
    }

    @Override
    public ResultSet executeQuery(String sql,
                                  Map<MidasQueryParam, String> additionalDBParams,
                                  List<MidasExternalData> externalData,
                                  Map<String, String> additionalRequestParams) throws SQLException {

        // forcibly disable extremes for ResultSet queries
        if (additionalDBParams == null || additionalDBParams.isEmpty()) {
            additionalDBParams = new EnumMap<>(MidasQueryParam.class);
        } else {
            additionalDBParams = new EnumMap<>(additionalDBParams);
        }
        additionalDBParams.put(MidasQueryParam.EXTREMES, "0");

        parseSingleStatement(sql, MidasFormat.TabSeparatedWithNamesAndTypes);
        if (!parsedStmt.isRecognized() && isSelect(sql)) {
            Map<String, Integer> positions = new HashMap<>();
            String dbName = extractDBName(sql);
            String tableName = extractTableName(sql);
            if (extractWithTotals(sql)) {
                positions.put(MidasSqlStatement.KEYWORD_TOTALS, 1);
            }
            parsedStmt = new MidasSqlStatement(sql, StatementType.SELECT,
                null, dbName, tableName, null, null, null, positions);
            // httpContext.setAttribute("is_idempotent", Boolean.TRUE);
        }

        InputStream is = getInputStream(sql, additionalDBParams, externalData, additionalRequestParams);
        
        try {
            if (parsedStmt.isQuery()) {
                currentUpdateCount = -1;
                currentResult = createResultSet(properties.isCompress()
                    ? new MidasLZ4Stream(is) : is, properties.getBufferSize(),
                    parsedStmt.getDatabaseOrDefault(properties.getDatabase()),
                    parsedStmt.getTable(),
                    parsedStmt.hasWithTotals(),
                    this,
                    getConnection().getTimeZone(),
                    properties
                );
                currentResult.setMaxRows(maxRows);
                return currentResult;
            } else {
                currentUpdateCount = 0;
                StreamUtils.close(is);
                return null;
            }
        } catch (Exception e) {
            StreamUtils.close(is);
            throw MidasExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public MidasResponse executeQueryMidasResponse(String sql) throws SQLException {
        return executeQueryMidasResponse(sql, null);
    }

    @Override
    public MidasResponse executeQueryMidasResponse(String sql, Map<MidasQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryMidasResponse(sql, additionalDBParams, null);
    }

    @Override
    public MidasResponse executeQueryMidasResponse(String sql,
                                                             Map<MidasQueryParam, String> additionalDBParams,
                                                             Map<String, String> additionalRequestParams) throws SQLException {
        parseSingleStatement(sql, MidasFormat.JSONCompact);
        if (parsedStmt.isRecognized()) {
            sql = parsedStmt.getSQL();
        } else {
            sql = addFormatIfAbsent(sql, MidasFormat.JSONCompact);
        }

        InputStream is = getInputStream(
                sql,
                additionalDBParams,
                null,
                additionalRequestParams
        );
        try {
            if (properties.isCompress()) {
                is = new MidasLZ4Stream(is);
            }
            return Jackson.getObjectMapper().readValue(is, MidasResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            StreamUtils.close(is);
        }
    }

    @Override
    public MidasRowBinaryInputStream executeQueryMidasRowBinaryStream(String sql) throws SQLException {
        return executeQueryMidasRowBinaryStream(sql, null);
    }

    @Override
    public MidasRowBinaryInputStream executeQueryMidasRowBinaryStream(String sql, Map<MidasQueryParam, String> additionalDBParams) throws SQLException {
        return executeQueryMidasRowBinaryStream(sql, additionalDBParams, null);
    }

    @Override
    public MidasRowBinaryInputStream executeQueryMidasRowBinaryStream(String sql, Map<MidasQueryParam, String> additionalDBParams, Map<String, String> additionalRequestParams) throws SQLException {
        parseSingleStatement(sql, MidasFormat.RowBinary);
        if (parsedStmt.isRecognized()) {
            sql = parsedStmt.getSQL();
        } else {
            sql = addFormatIfAbsent(sql, MidasFormat.RowBinary);
            if (isSelect(sql)) {
                parsedStmt = new MidasSqlStatement(sql, StatementType.SELECT);
                // httpContext.setAttribute("is_idempotent", Boolean.TRUE);
            } else {
                parsedStmt = new MidasSqlStatement(sql, StatementType.UNKNOWN);
            }
        }

        InputStream is = getInputStream(
                sql,
                additionalDBParams,
                null,
                additionalRequestParams
        );
        try {
            if (parsedStmt.isQuery()) {
                currentUpdateCount = -1;
                currentRowBinaryResult = new MidasRowBinaryInputStream(properties.isCompress()
                        ? new MidasLZ4Stream(is) : is, getConnection().getTimeZone(), properties);
                return currentRowBinaryResult;
            } else {
                currentUpdateCount = 0;
                StreamUtils.close(is);
                return null;
            }
        } catch (Exception e) {
            StreamUtils.close(is);
            throw MidasExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        parseSingleStatement(sql, MidasFormat.TabSeparatedWithNamesAndTypes);

        InputStream is = null;
        try {
            is = getInputStream(sql, null, null, null);
            //noinspection StatementWithEmptyBody
        } finally {
            StreamUtils.close(is);
        }

        return currentSummary != null ? (int) currentSummary.getWrittenRows() : 1;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        // currentResult is stored here. InputString and currentResult will be closed on this.close()
        return executeQuery(sql) != null;
    }

    @Override
    public void close() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
        }

        if (currentRowBinaryResult != null) {
            StreamUtils.close(currentRowBinaryResult);
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException(String.format("Illegal maxRows value: %d", max));
        }
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        queryTimeout = seconds;
        isQueryTimeoutSet = true;
    }

    @Override
    public void cancel() throws SQLException {
        if (this.queryId == null || isClosed()) {
            return;
        }

       executeQuery(String.format("KILL QUERY WHERE query_id='%s'", queryId));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResult;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return currentUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
            currentResult = null;
        }
        currentUpdateCount = -1;
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public MidasConnection getConnection() throws MidasException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
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
    public MidasResponseSummary getResponseSummary() {
        return currentSummary;
    }

    @Deprecated
    static String clickhousifySql(String sql) {
        return addFormatIfAbsent(sql, MidasFormat.TabSeparatedWithNamesAndTypes);
    }

    /**
     * Adding  FORMAT TabSeparatedWithNamesAndTypes if not added
     * adds format only to select queries
     */
    @Deprecated
    private static String addFormatIfAbsent(final String sql, MidasFormat format) {
        String cleanSQL = sql.trim();
        if (!isSelect(cleanSQL)) {
            return cleanSQL;
        }
        if (MidasFormat.containsFormat(cleanSQL)) {
            return cleanSQL;
        }
        StringBuilder sb = new StringBuilder();
        int idx = cleanSQL.endsWith(";")
            ? cleanSQL.length() - 1
            : cleanSQL.length();
        sb.append(cleanSQL, 0, idx)
          .append("\nFORMAT ")
          .append(format.name())
          .append(';');
        return sb.toString();
    }

    @Deprecated
    static boolean isSelect(String sql) {
        for (int i = 0; i < sql.length(); i++) {
            String nextTwo = sql.substring(i, Math.min(i + 2, sql.length()));
            if ("--".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf("\n", i));
            } else if ("/*".equals(nextTwo)) {
                i = Math.max(i, sql.indexOf("*/", i));
            } else if (Character.isLetter(sql.charAt(i))) {
                String trimmed = sql.substring(i, Math.min(sql.length(), Math.max(i, sql.indexOf(" ", i))));
                for (String keyword : selectKeywords){
                    if (trimmed.regionMatches(true, 0, keyword, 0, keyword.length())) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    @Deprecated
    private String extractTableName(String sql) {
        String s = extractDBAndTableName(sql);
        if (s.contains(".")) {
            return s.substring(s.indexOf(".") + 1);
        } else {
            return s;
        }
    }

    @Deprecated
    private String extractDBName(String sql) {
        String s = extractDBAndTableName(sql);
        if (s.contains(".")) {
            return s.substring(0, s.indexOf("."));
        } else {
            return properties.getDatabase();
        }
    }

    @Deprecated
    private String extractDBAndTableName(String sql) {
        if (Utils.startsWithIgnoreCase(sql, "select")) {
            String withoutStrings = Utils.retainUnquoted(sql, '\'');
            int fromIndex = withoutStrings.indexOf("from");
            if (fromIndex == -1) {
                fromIndex = withoutStrings.indexOf("FROM");
            }
            if (fromIndex != -1) {
                String fromFrom = withoutStrings.substring(fromIndex);
                String fromTable = fromFrom.substring("from".length()).trim();
                return fromTable.split(" ")[0];
            }
        }
        if (Utils.startsWithIgnoreCase(sql, "desc")) {
            return "system.columns";
        }
        if (Utils.startsWithIgnoreCase(sql, "show")) {
            return "system.tables";
        }
        return "system.unknown";
    }

    @Deprecated
    private boolean extractWithTotals(String sql) {
        if (Utils.startsWithIgnoreCase(sql, "select")) {
            String withoutStrings = Utils.retainUnquoted(sql, '\'');
            return withoutStrings.toLowerCase(Locale.ROOT).contains(" with totals");
        }
        return false;
    }

    private InputStream getInputStream(
        String sql,
        Map<MidasQueryParam, String> additionalMidasDBParams,
        List<MidasExternalData> externalData,
        Map<String, String> additionalRequestParams
    ) throws MidasException {
        boolean ignoreDatabase = false;
        if (parsedStmt.isRecognized()) {
            sql = parsedStmt.getSQL();
            // TODO consider more scenarios like drop, show etc.
            ignoreDatabase = parsedStmt.getStatementType() == StatementType.CREATE 
                && parsedStmt.containsKeyword(MidasSqlStatement.KEYWORD_DATABASE);
        } else {
            sql = clickhousifySql(sql);
            ignoreDatabase = sql.trim().regionMatches(true, 0, databaseKeyword, 0, databaseKeyword.length());
        }
        log.debug("Executing SQL: {}", sql);

        additionalMidasDBParams = addQueryIdTo(
                additionalMidasDBParams == null
                        ? new EnumMap<MidasQueryParam, String>(MidasQueryParam.class)
                        : additionalMidasDBParams);

        URI uri;
        if (externalData == null || externalData.isEmpty()) {
            uri = buildRequestUri(
                    null,
                    null,
                    additionalMidasDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            );
        } else {
            // write sql in query params when there is external data
            // as it is impossible to pass both external data and sql in body
            // TODO move sql to request body when it is supported in Midas
            uri = buildRequestUri(
                    sql,
                    externalData,
                    additionalMidasDBParams,
                    additionalRequestParams,
                    ignoreDatabase
            );
        }
        log.debug("Request url: {}", uri);


        HttpEntity requestEntity;
        if (externalData == null || externalData.isEmpty()) {
            requestEntity = new StringEntity(sql, StreamUtils.UTF_8);
        } else {
            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();

            try {
                for (MidasExternalData externalDataItem : externalData) {
                    // Midas may return 400 (bad request) when chunked encoding is used with multipart request
                    // so read content to byte array to avoid chunked encoding
                    // TODO do not read stream into memory when this issue is fixed in Midas
                    entityBuilder.addBinaryBody(
                        externalDataItem.getName(),
                        StreamUtils.toByteArray(externalDataItem.getContent()),
                        ContentType.APPLICATION_OCTET_STREAM,
                        externalDataItem.getName()
                    );
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            requestEntity = entityBuilder.build();
        }

        requestEntity = applyRequestBodyCompression(requestEntity);

        HttpEntity entity = null;
        try {
            uri = followRedirects(uri);
            HttpPost post = new HttpPost(uri);
            post.setEntity(requestEntity);

            HttpResponse response = client.execute(post, httpContext);
            entity = response.getEntity();
            checkForErrorAndThrow(entity, response);

            InputStream is;
            if (entity.isStreaming()) {
                is = entity.getContent();
            } else {
                FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
                entity.writeTo(baos);
                is = baos.convertToInputStream();
            }

            // retrieve response summary
            if (isQueryParamSet(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, additionalMidasDBParams, additionalRequestParams)) {
                Header summaryHeader = response.getFirstHeader("X-Midas-Summary");
                currentSummary = summaryHeader != null ? Jackson.getObjectMapper().readValue(summaryHeader.getValue(), MidasResponseSummary.class) : null;
            }

            return is;
        } catch (MidasException e) {
            throw e;
        } catch (Exception e) {
            log.info("Error during connection to {}, reporting failure to data source, message: {}", properties, e.getMessage());
            EntityUtils.consumeQuietly(entity);
            log.info("Error sql: {}", sql);
            throw MidasExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        }
    }

    URI buildRequestUri(
        String sql,
        List<MidasExternalData> externalData,
        Map<MidasQueryParam, String> additionalMidasDBParams,
        Map<String, String> additionalRequestParams,
        boolean ignoreDatabase
    ) {
        try {
            List<NameValuePair> queryParams = getUrlQueryParams(
                sql,
                externalData,
                additionalMidasDBParams,
                additionalRequestParams,
                ignoreDatabase
            );

            return new URIBuilder()
                .setScheme(properties.getSsl() ? "https" : "http")
                .setHost(properties.getHost())
                .setPort(properties.getPort())
                .setPath((properties.getPath() == null || properties.getPath().isEmpty() ? "/" : properties.getPath()))
                .setParameters(queryParams)
                .build();
        } catch (URISyntaxException e) {
            log.error("Mailformed URL: {}", e.getMessage());
            throw new IllegalStateException("illegal configuration of db");
        }
    }

    private List<NameValuePair> getUrlQueryParams(
        String sql,
        List<MidasExternalData> externalData,
        Map<MidasQueryParam, String> additionalMidasDBParams,
        Map<String, String> additionalRequestParams,
        boolean ignoreDatabase
    ) {
        List<NameValuePair> result = new ArrayList<>();

        if (sql != null) {
            result.add(new BasicNameValuePair("query", sql));
        }

        if (externalData != null) {
            for (MidasExternalData externalDataItem : externalData) {
                String name = externalDataItem.getName();
                String format = externalDataItem.getFormat();
                String types = externalDataItem.getTypes();
                String structure = externalDataItem.getStructure();

                if (format != null && !format.isEmpty()) {
                    result.add(new BasicNameValuePair(name + "_format", format));
                }
                if (types != null && !types.isEmpty()) {
                    result.add(new BasicNameValuePair(name + "_types", types));
                }
                if (structure != null && !structure.isEmpty()) {
                    result.add(new BasicNameValuePair(name + "_structure", structure));
                }
            }
        }

        Map<MidasQueryParam, String> params = properties.buildQueryParams(true);
        if (!ignoreDatabase) {
            params.put(MidasQueryParam.DATABASE, initialDatabase);
        }

        params.putAll(getAdditionalDBParams());

        if (additionalMidasDBParams != null && !additionalMidasDBParams.isEmpty()) {
            params.putAll(additionalMidasDBParams);
        }

        setStatementPropertiesToParams(params);

        for (Map.Entry<MidasQueryParam, String> entry : params.entrySet()) {
            if (!Strings.isNullOrEmpty(entry.getValue())) {
                result.add(new BasicNameValuePair(entry.getKey().toString(), entry.getValue()));
            }
        }

        for (Map.Entry<String, String> entry : getRequestParams().entrySet()) {
            if (!Strings.isNullOrEmpty(entry.getValue())) {
                result.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
        }

        if (additionalRequestParams != null) {
            for (Map.Entry<String, String> entry : additionalRequestParams.entrySet()) {
                if (!Strings.isNullOrEmpty(entry.getValue())) {
                    result.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
                }
            }
        }

        return result;
    }

    private boolean isQueryParamSet(MidasQueryParam param, Map<MidasQueryParam, String> additionalMidasDBParams, Map<String, String> additionalRequestParams) {
        String value = getQueryParamValue(param, additionalMidasDBParams, additionalRequestParams);

        return "true".equals(value) || "1".equals(value);
    }

    private String getQueryParamValue(MidasQueryParam param, Map<MidasQueryParam, String> additionalMidasDBParams, Map<String, String> additionalRequestParams) {
        if (additionalRequestParams != null && additionalRequestParams.containsKey(param.getKey()) && !Strings.isNullOrEmpty(additionalRequestParams.get(param.getKey()))) {
            return additionalRequestParams.get(param.getKey());
        }

        if (getRequestParams().containsKey(param.getKey()) && !Strings.isNullOrEmpty(getRequestParams().get(param.getKey()))) {
            return getRequestParams().get(param.getKey());
        }

        if (additionalMidasDBParams != null && additionalMidasDBParams.containsKey(param) && !Strings.isNullOrEmpty(additionalMidasDBParams.get(param))) {
            return additionalMidasDBParams.get(param);
        }

        if (getAdditionalDBParams().containsKey(param) && !Strings.isNullOrEmpty(getAdditionalDBParams().get(param))) {
            return getAdditionalDBParams().get(param);
        }

        return properties.asProperties().getProperty(param.getKey());
    }

    private URI followRedirects(URI uri) throws IOException, URISyntaxException {
        if (properties.isCheckForRedirects()) {
            int redirects = 0;
            while (redirects < properties.getMaxRedirects()) {
                HttpGet httpGet = new HttpGet(uri);
                HttpResponse response = client.execute(httpGet, httpContext);
                if (response.getStatusLine().getStatusCode() == 307) {
                    uri = new URI(response.getHeaders("Location")[0].getValue());
                    redirects++;
                    log.info("Redirected to " + uri.getHost());
                } else {
                    break;
                }
            }
        }
        return uri;
    }

    private void setStatementPropertiesToParams(Map<MidasQueryParam, String> params) {
        if (maxRows > 0) {
            params.put(MidasQueryParam.MAX_RESULT_ROWS, String.valueOf(maxRows));
            params.put(MidasQueryParam.RESULT_OVERFLOW_MODE, "break");
        }
        if(isQueryTimeoutSet) {
            params.put(MidasQueryParam.MAX_EXECUTION_TIME, String.valueOf(queryTimeout));
        }
    }


    @Override
    public void sendRowBinaryStream(String sql, MidasStreamCallback callback) throws SQLException {
        sendRowBinaryStream(sql, null, callback);
    }

    @Override
    public void sendRowBinaryStream(String sql, Map<MidasQueryParam, String> additionalDBParams, MidasStreamCallback callback) throws SQLException {
        write().withDbParams(additionalDBParams)
                .send(sql, callback, MidasFormat.RowBinary);
    }

    @Override
    public void sendNativeStream(String sql, MidasStreamCallback callback) throws SQLException {
        sendNativeStream(sql, null, callback);
    }

    @Override
    public void sendNativeStream(String sql, Map<MidasQueryParam, String> additionalDBParams, MidasStreamCallback callback) throws SQLException {
        write().withDbParams(additionalDBParams)
                .send(sql, callback, MidasFormat.Native);
    }

    @Override
    public void sendCSVStream(InputStream content, String table, Map<MidasQueryParam, String> additionalDBParams) throws SQLException {
        write()
                .table(table)
                .withDbParams(additionalDBParams)
                .data(content)
                .format(MidasFormat.CSV)
                .send();
    }

    @Override
    public void sendCSVStream(InputStream content, String table) throws SQLException {
        sendCSVStream(content, table, null);
    }

    @Override
    public void sendStream(InputStream content, String table) throws SQLException {
        sendStream(content, table, null);
    }

    @Override
    public void sendStream(InputStream content, String table,
                           Map<MidasQueryParam, String> additionalDBParams) throws SQLException {
        write()
                .table(table)
                .data(content)
                .withDbParams(additionalDBParams)
                .format(MidasFormat.TabSeparated)
                .send();
    }

    @Deprecated
    public void sendStream(HttpEntity content, String sql) throws MidasException {
        sendStream(content, sql, MidasFormat.TabSeparated, null);
    }

    @Deprecated
    public void sendStream(HttpEntity content, String sql,
                           Map<MidasQueryParam, String> additionalDBParams) throws MidasException {
        sendStream(content, sql, MidasFormat.TabSeparated, additionalDBParams);
    }

    private void sendStream(HttpEntity content, String sql, MidasFormat format,
                            Map<MidasQueryParam, String> additionalDBParams) throws MidasException {

        Writer writer = write().format(format).withDbParams(additionalDBParams).sql(sql);
        sendStream(writer, content);
    }

    @Override
    public void sendStreamSQL(InputStream content, String sql,
                              Map<MidasQueryParam, String> additionalDBParams) throws SQLException {
        write().data(content).sql(sql).withDbParams(additionalDBParams).send();
    }

    @Override
    public void sendStreamSQL(InputStream content, String sql) throws SQLException {
        write().sql(sql).data(content).send();
    }

    void sendStream(Writer writer, HttpEntity content) throws MidasException {
        HttpEntity entity = null;
        try {

            URI uri = buildRequestUri(writer.getSql(), null, writer.getAdditionalDBParams(), writer.getRequestParams(), false);
            uri = followRedirects(uri);

            content = applyRequestBodyCompression(content);

            HttpPost httpPost = new HttpPost(uri);

            if (writer.getCompression() != null) {
                httpPost.addHeader("Content-Encoding", writer.getCompression().name());
            }
            httpPost.setEntity(content);
            HttpResponse response = client.execute(httpPost, httpContext);
            entity = response.getEntity();
            checkForErrorAndThrow(entity, response);

            // retrieve response summary
            if (isQueryParamSet(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, writer.getAdditionalDBParams(), writer.getRequestParams())) {
                Header summaryHeader = response.getFirstHeader("X-Midas-Summary");
                currentSummary = summaryHeader != null ? Jackson.getObjectMapper().readValue(summaryHeader.getValue(), MidasResponseSummary.class) : null;
            }
        } catch (MidasException e) {
            throw e;
        } catch (Exception e) {
            throw MidasExceptionSpecifier.specify(e, properties.getHost(), properties.getPort());
        } finally {
            EntityUtils.consumeQuietly(entity);
        }
    }

    private void checkForErrorAndThrow(HttpEntity entity, HttpResponse response) throws IOException, MidasException {
        if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_OK) {
            InputStream messageStream = entity.getContent();
            byte[] bytes = StreamUtils.toByteArray(messageStream);
            if (properties.isCompress()) {
                try {
                    messageStream = new MidasLZ4Stream(new ByteArrayInputStream(bytes));
                    bytes = StreamUtils.toByteArray(messageStream);
                } catch (IOException e) {
                    log.warn("error while read compressed stream {}", e.getMessage());
                }
            }
            EntityUtils.consumeQuietly(entity);
            String chMessage = new String(bytes, StreamUtils.UTF_8);
            throw MidasExceptionSpecifier.specify(chMessage, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }

    private HttpEntity applyRequestBodyCompression(final HttpEntity entity) {
        if (properties.isDecompress()) {
            return new LZ4EntityWrapper(entity, properties.getMaxCompressBufferSize());
        }
        return entity;
    }

    private MidasResultSet createResultSet(InputStream is, int bufferSize, String db, String table, boolean usesWithTotals,
    		MidasStatement statement, TimeZone timezone, MidasProperties properties) throws IOException {
    	if(isResultSetScrollable) {
    		return new MidasScrollableResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    	} else {
    		return new MidasResultSet(is, bufferSize, db, table, usesWithTotals, statement, timezone, properties);
    	}
    }

    private Map<MidasQueryParam, String> addQueryIdTo(Map<MidasQueryParam, String> parameters) {
        if (this.queryId != null) {
            return parameters;
        }

        String queryId = parameters.get(MidasQueryParam.QUERY_ID);
        if (queryId == null) {
            this.queryId = UUID.randomUUID().toString();
            parameters.put(MidasQueryParam.QUERY_ID, this.queryId);
        } else {
            this.queryId = queryId;
        }

        return parameters;
    }

    @Override
    public Writer write() {
        return new Writer(this).withDbParams(getAdditionalDBParams()).options(getRequestParams());
    }
}
