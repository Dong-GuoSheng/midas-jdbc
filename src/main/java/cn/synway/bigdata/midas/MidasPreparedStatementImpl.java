package cn.synway.bigdata.midas;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import cn.synway.bigdata.midas.jdbc.parser.MidasSqlStatement;
import cn.synway.bigdata.midas.jdbc.parser.StatementType;
import cn.synway.bigdata.midas.response.MidasResponse;
import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.settings.MidasQueryParam;
import cn.synway.bigdata.midas.util.MidasArrayUtil;
import cn.synway.bigdata.midas.util.MidasValueFormatter;
import cn.synway.bigdata.midas.util.guava.StreamUtils;

public class MidasPreparedStatementImpl extends MidasStatementImpl implements MidasPreparedStatement {

    static final String PARAM_MARKER = "?";
    static final String NULL_MARKER = "\\N";

    private static final Pattern VALUES = Pattern.compile("(?i)VALUES[\\s]*\\(");

    private final TimeZone dateTimeZone;
    private final TimeZone dateTimeTimeZone;
    private final String sql;
    private final List<String> sqlParts;
    private final MidasPreparedStatementParameter[] binds;
    private final List<List<String>> parameterList;
    private final boolean insertBatchMode;
    private List<byte[]> batchRows = new ArrayList<>();

    public MidasPreparedStatementImpl(CloseableHttpClient client,
        MidasConnection connection, MidasProperties properties, String sql,
        TimeZone serverTimeZone, int resultSetType) throws SQLException
    {
        super(client, connection, properties, resultSetType);
        parseSingleStatement(sql);

        this.sql = sql;
        PreparedStatementParser parser = PreparedStatementParser.parse(sql,
            parsedStmt.getEndPosition(MidasSqlStatement.KEYWORD_VALUES));
        this.parameterList = parser.getParameters();
        this.insertBatchMode = parser.isValuesMode();
        this.sqlParts = parser.getParts();
        int numParams = countNonConstantParams();
        this.binds = new MidasPreparedStatementParameter[numParams];
        dateTimeTimeZone = serverTimeZone;
        if (properties.isUseServerTimeZoneForDates()) {
            dateTimeZone = serverTimeZone;
        } else {
            dateTimeZone = TimeZone.getDefault();
        }
    }

    @Override
    public void clearParameters() {
        Arrays.fill(binds, null);
    }

    @Override
    public MidasResponse executeQueryMidasResponse() throws SQLException {
        return super.executeQueryMidasResponse(buildSql());
    }

    @Override
    public MidasResponse executeQueryMidasResponse(Map<MidasQueryParam, String> additionalDBParams) throws SQLException {
        return super.executeQueryMidasResponse(buildSql(), additionalDBParams);
    }

    private String buildSql() throws SQLException {
        if (sqlParts.size() == 1) {
            return sqlParts.get(0);
        }
        checkBinded();
        StringBuilder sb = new StringBuilder(sqlParts.get(0));
        for (int i = 1, p = 0; i < sqlParts.size(); i++) {
            String pValue = getParameter(i - 1);
            if (PARAM_MARKER.equals(pValue)) {
                sb.append(binds[p++].getRegularValue());
            } else if (NULL_MARKER.equals(pValue)) {
                sb.append("NULL");
            } else {
                sb.append(pValue);
            }
            sb.append(sqlParts.get(i));
        }
        return sb.toString();
    }

    private void checkBinded() throws SQLException {
        int i = 0;
        for (Object b : binds) {
            ++i;
            if (b == null) {
                throw new SQLException("Not all parameters binded (placeholder " + i + " is undefined)");
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(buildSql());
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(buildSql());
    }

    @Override
    public void clearBatch() throws SQLException {
        batchRows.clear();
    }

    @Override
    public ResultSet executeQuery(Map<MidasQueryParam, String> additionalDBParams) throws SQLException {
        return super.executeQuery(buildSql(), additionalDBParams);
    }

    @Override
    public ResultSet executeQuery(Map<MidasQueryParam, String> additionalDBParams, List<MidasExternalData> externalData) throws SQLException {
        return super.executeQuery(buildSql(), additionalDBParams, externalData);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(buildSql());
    }

    private void setBind(int parameterIndex, String bind, boolean quote) {
        binds[parameterIndex - 1] = new MidasPreparedStatementParameter(bind, quote);
    }

    private void setBind(int parameterIndex, MidasPreparedStatementParameter parameter) {
        binds[parameterIndex -1] = parameter;
    }

    private void setNull(int parameterIndex) {
        setBind(parameterIndex, MidasPreparedStatementParameter.nullParameter());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setNull(parameterIndex);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setBind(parameterIndex, MidasPreparedStatementParameter.boolParameter(x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setBind(parameterIndex, MidasValueFormatter.formatByte(x), false);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setBind(parameterIndex, MidasValueFormatter.formatShort(x), false);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setBind(parameterIndex, MidasValueFormatter.formatInt(x), false);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setBind(parameterIndex, MidasValueFormatter.formatLong(x), false);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setBind(parameterIndex, MidasValueFormatter.formatFloat(x), false);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setBind(parameterIndex, MidasValueFormatter.formatDouble(x), false);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setBind(parameterIndex, MidasValueFormatter.formatBigDecimal(x), false);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setBind(parameterIndex, MidasValueFormatter.formatString(x), x != null);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setBind(parameterIndex, MidasValueFormatter.formatBytes(x), true);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        if (x != null) {
            setBind(
                parameterIndex,
                MidasValueFormatter.formatDate(x, dateTimeZone),
                true);
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        if (x != null) {
            setBind(
                parameterIndex,
                MidasValueFormatter.formatTime(x, dateTimeTimeZone),
                true);
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        if (x != null) {
            setBind(
                parameterIndex,
                MidasValueFormatter.formatTimestamp(x, dateTimeTimeZone),
                true);
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setArray(int parameterIndex, Collection collection) throws SQLException {
        setBind(parameterIndex, MidasArrayUtil.toString(collection, dateTimeZone, dateTimeTimeZone),
            false);
    }

    @Override
    public void setArray(int parameterIndex, Object[] array) throws SQLException {
        setBind(parameterIndex, MidasArrayUtil.toString(array, dateTimeZone, dateTimeTimeZone), false);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setBind(parameterIndex, MidasArrayUtil.arrayToString(x.getArray(), dateTimeZone, dateTimeTimeZone),
            false);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x != null) {
            setBind(
                parameterIndex,
                MidasPreparedStatementParameter.fromObject(
                    x, dateTimeZone, dateTimeTimeZone));
        } else {
            setNull(parameterIndex);
        }
    }

    @Override
    public void addBatch() throws SQLException {
        batchRows.addAll(buildBatch());
    }

    private List<byte[]> buildBatch() throws SQLException {
        checkBinded();
        List<byte[]> newBatches = new ArrayList<>(parameterList.size());
        StringBuilder sb = new StringBuilder();
        for (int i = 0, p = 0; i < parameterList.size(); i++) {
            List<String> pList = parameterList.get(i);
            for (int j = 0; j < pList.size(); j++) {
                String pValue = pList.get(j);
                if (PARAM_MARKER.equals(pValue)) {
                    if (insertBatchMode) {
                        sb.append(binds[p++].getBatchValue());
                    } else {
                        sb.append(binds[p++].getRegularValue());
                    }
                } else {
                    sb.append(pValue);
                }
                sb.append(j < pList.size() - 1 ? "\t" : "\n");
            }
            newBatches.add(sb.toString().getBytes(StreamUtils.UTF_8));
            sb = new StringBuilder();
        }
        return newBatches;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return executeBatch(null);
    }

    @Override
    public int[] executeBatch(Map<MidasQueryParam, String> additionalDBParams) throws SQLException {
        int valuePosition = -1;
        if (parsedStmt.getStatementType() == StatementType.INSERT && parsedStmt.hasValues()) {
            valuePosition = parsedStmt.getStartPosition(MidasSqlStatement.KEYWORD_VALUES);
        } else {
            Matcher matcher = VALUES.matcher(sql);
            if (matcher.find()) {
                valuePosition = matcher.start();
            }    
        }

        if (valuePosition < 0) {
            throw new SQLSyntaxErrorException(
                    "Query must be like 'INSERT INTO [db.]table [(c1, c2, c3)] VALUES (?, ?, ?)'. " +
                            "Got: " + sql
            );
        }
        String insertSql = sql.substring(0, valuePosition);
        BatchHttpEntity entity = new BatchHttpEntity(batchRows);
        sendStream(entity, insertSql, additionalDBParams);
        int[] result = new int[batchRows.size()];
        Arrays.fill(result, 1);
        batchRows = new ArrayList<>();
        return result;
    }

    private static class BatchHttpEntity extends AbstractHttpEntity {
        private final List<byte[]> rows;

        public BatchHttpEntity(List<byte[]> rows) {
            this.rows = rows;
        }

        @Override
        public boolean isRepeatable() {
            return true;
        }

        @Override
        public long getContentLength() {
            return -1;
        }

        @Override
        public InputStream getContent() throws IOException, IllegalStateException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeTo(OutputStream outputStream) throws IOException {
            for (byte[] row : rows) {
                outputStream.write(row);
            }
        }

        @Override
        public boolean isStreaming() {
            return false;
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet currentResult = getResultSet();
        if (currentResult != null) {
            return currentResult.getMetaData();
        }
        
        if (!parsedStmt.isQuery() || (!parsedStmt.isRecognized() && !isSelect(sql))) {
            return null;
        }
        ResultSet myRs = executeQuery(Collections.singletonMap(
            MidasQueryParam.MAX_RESULT_ROWS, "0"));
        return myRs != null ? myRs.getMetaData() : null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private int countNonConstantParams() {
        int count = 0;
        for (int i = 0; i < parameterList.size(); i++) {
            List<String> pList = parameterList.get(i);
            for (int j = 0; j < pList.size(); j++) {
                if (PARAM_MARKER.equals(pList.get(j))) {
                    count += 1;
                }
            }
        }
        return count;
    }

    private String getParameter(int paramIndex) {
        for (int i = 0, count = paramIndex; i < parameterList.size(); i++) {
            List<String> pList = parameterList.get(i);
            count -= pList.size();
            if (count < 0) {
                return pList.get(pList.size() + count);
            }
        }
        return null;
    }

    @Override
    public String asSql() {
        try {
            return buildSql();
        } catch (SQLException e) {
            return sql;
        }
    }
}
