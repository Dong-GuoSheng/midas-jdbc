package cn.synway.bigdata.midas;

import cn.synway.bigdata.midas.response.MidasResponse;
import cn.synway.bigdata.midas.settings.MidasQueryParam;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public interface MidasPreparedStatement extends PreparedStatement, MidasStatement {
    MidasResponse executeQueryMidasResponse() throws SQLException;

    MidasResponse executeQueryMidasResponse(Map<MidasQueryParam, String> additionalDBParams) throws SQLException;

    void setArray(int parameterIndex, Collection collection) throws SQLException;

    void setArray(int parameterIndex, Object[] array) throws SQLException;

    ResultSet executeQuery(Map<MidasQueryParam, String> additionalDBParams) throws SQLException;

    ResultSet executeQuery(Map<MidasQueryParam, String> additionalDBParams, List<MidasExternalData> externalData) throws SQLException;

    int[] executeBatch(Map<MidasQueryParam, String> additionalDBParams) throws SQLException;

    String asSql();
}
