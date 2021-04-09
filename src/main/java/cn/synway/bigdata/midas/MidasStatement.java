package cn.synway.bigdata.midas;

import cn.synway.bigdata.midas.response.MidasResponse;
import cn.synway.bigdata.midas.response.MidasResponseSummary;
import cn.synway.bigdata.midas.settings.MidasQueryParam;
import cn.synway.bigdata.midas.util.MidasRowBinaryInputStream;
import cn.synway.bigdata.midas.util.MidasStreamCallback;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;


public interface MidasStatement extends Statement {

    MidasResponse executeQueryMidasResponse(String sql) throws SQLException;

    MidasResponse executeQueryMidasResponse(String sql, Map<MidasQueryParam, String> additionalDBParams) throws SQLException;

    MidasResponse executeQueryMidasResponse(String sql,
                                                      Map<MidasQueryParam, String> additionalDBParams,
                                                      Map<String, String> additionalRequestParams) throws SQLException;

    MidasRowBinaryInputStream executeQueryMidasRowBinaryStream(String sql) throws SQLException;

    MidasRowBinaryInputStream executeQueryMidasRowBinaryStream(String sql,
                                                                         Map<MidasQueryParam, String> additionalDBParams) throws SQLException;

    MidasRowBinaryInputStream executeQueryMidasRowBinaryStream(String sql,
                                                                         Map<MidasQueryParam, String> additionalDBParams,
                                                                         Map<String, String> additionalRequestParams) throws SQLException;

    ResultSet executeQuery(String sql, Map<MidasQueryParam, String> additionalDBParams) throws SQLException;

    ResultSet executeQuery(String sql, Map<MidasQueryParam, String> additionalDBParams, List<MidasExternalData> externalData) throws SQLException;

    ResultSet executeQuery(String sql,
                           Map<MidasQueryParam, String> additionalDBParams,
                           List<MidasExternalData> externalData,
                           Map<String, String> additionalRequestParams) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendStream(InputStream content, String table, Map<MidasQueryParam, String> additionalDBParams) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendStream(InputStream content, String table) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendRowBinaryStream(String sql, Map<MidasQueryParam, String> additionalDBParams, MidasStreamCallback callback) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendRowBinaryStream(String sql, MidasStreamCallback callback) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendNativeStream(String sql, Map<MidasQueryParam, String> additionalDBParams, MidasStreamCallback callback) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendNativeStream(String sql, MidasStreamCallback callback) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendCSVStream(InputStream content, String table, Map<MidasQueryParam, String> additionalDBParams) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendCSVStream(InputStream content, String table) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendStreamSQL(InputStream content, String sql, Map<MidasQueryParam, String> additionalDBParams) throws SQLException;

    /**
     * @see #write()
     */
    @Deprecated
    void sendStreamSQL(InputStream content, String sql) throws SQLException;

    /**
     * Returns extended write-API
     */
    Writer write();

    MidasResponseSummary getResponseSummary();
}
