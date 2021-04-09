package cn.synway.bigdata.midas;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.TimeZone;


public interface MidasConnection extends Connection {

    @Deprecated
    MidasStatement createMidasStatement() throws SQLException;

    TimeZone getTimeZone();

    @Override
    MidasStatement createStatement() throws SQLException;

    @Override
    MidasStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException;

    String getServerVersion() throws SQLException;
}
