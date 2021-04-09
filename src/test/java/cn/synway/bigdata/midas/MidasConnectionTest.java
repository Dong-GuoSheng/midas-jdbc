package cn.synway.bigdata.midas;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import cn.synway.bigdata.midas.domain.MidasDataType;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class MidasConnectionTest {
    @Test
    public void testGetSetCatalog() throws SQLException {
        String address = MidasContainerForTest.getMidasHttpAddress();
        String url = "jdbc:midas://" + address + "/default?option1=one%20two&option2=y";
        MidasDataSource dataSource = MidasContainerForTest.newDataSource(url);
        String[] dbNames = new String[]{"get_set_catalog_test1", "get_set_catalog_test2"};
        try {
            MidasConnectionImpl connection = (MidasConnectionImpl) dataSource.getConnection();
            assertEquals(connection.getUrl(), url);
            assertEquals(connection.getCatalog(), "default");
            assertEquals(connection.getProperties().getDatabase(), "default");

            for (String db : dbNames) {
                connection.createStatement().executeUpdate("CREATE DATABASE " + db);
                connection.createStatement().executeUpdate(
                        "CREATE TABLE " + db + ".some_table ENGINE = TinyLog()"
                                + " AS SELECT 'value_" + db + "' AS field");

                connection.setCatalog(db);
                assertEquals(connection.getCatalog(), db);
                assertEquals(connection.getProperties().getDatabase(), db);
                assertEquals(connection.getUrl(),
                        "jdbc:midas://" + address + "/" + db + "?option1=one%20two&option2=y");

                ResultSet resultSet = connection.createStatement().executeQuery("SELECT field FROM some_table");
                assertTrue(resultSet.next());
                assertEquals(resultSet.getString(1), "value_" + db);
            }
        } finally {
            Connection connection = dataSource.getConnection();
            for (String db : dbNames) {
                connection.createStatement().executeUpdate("DROP DATABASE IF EXISTS " + db);
            }
        }
    }

    @Test
    public void testSetCatalogAndStatements() throws SQLException {
        MidasDataSource dataSource = MidasContainerForTest.newDataSource(
                "default?option1=one%20two&option2=y");
        MidasConnectionImpl connection = (MidasConnectionImpl) dataSource.getConnection();
        final String sql = "SELECT currentDatabase()";

        connection.setCatalog("system");
        Statement statement = connection.createStatement();
        connection.setCatalog("default");
        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        assertEquals(resultSet.getString(1), "system");

        statement = connection.createStatement();
        resultSet = statement.executeQuery(sql);
        resultSet.next();
        assertEquals(resultSet.getString(1), "default");
    }

    @Test
    public void testSetCatalogAndPreparedStatements() throws SQLException {
        MidasDataSource dataSource = MidasContainerForTest.newDataSource(
                "default?option1=one%20two&option2=y");
        MidasConnectionImpl connection = (MidasConnectionImpl) dataSource.getConnection();
        final String sql = "SELECT currentDatabase() FROM system.tables WHERE name = ? LIMIT 1";

        connection.setCatalog("system");
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, "tables");
        connection.setCatalog("default");
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        assertEquals(resultSet.getString(1), "system");

        statement = connection.prepareStatement(sql);
        statement.setString(1, "tables");
        resultSet = statement.executeQuery();
        resultSet.next();
        assertEquals(resultSet.getString(1), "default");
    }

    @Test
    public void testScrollableResultSetOnPreparedStatements() throws SQLException {
        MidasDataSource dataSource = MidasContainerForTest.newDataSource(
                "default?option1=one%20two&option2=y");
        MidasConnectionImpl connection = (MidasConnectionImpl) dataSource.getConnection();
        final String sql = "SELECT currentDatabase() FROM system.tables WHERE name = ? LIMIT 1";

        connection.setCatalog("system");
        PreparedStatement statement = connection.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setString(1, "tables");
        connection.setCatalog("default");
        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        assertEquals(resultSet.getString(1), "system");
        assertEquals(resultSet.getType(), ResultSet.TYPE_FORWARD_ONLY);

        statement = connection.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        statement.setString(1, "tables");
        resultSet = statement.executeQuery();
        resultSet.next();
        assertEquals(resultSet.getString(1), "default");
        assertEquals(resultSet.getType(), ResultSet.TYPE_SCROLL_INSENSITIVE);
        resultSet.beforeFirst();
        resultSet.next();
        assertEquals(resultSet.getString(1), "default");
    }

    @Test
    public void testScrollableResultSetOnStatements() throws SQLException {
        MidasDataSource dataSource = MidasContainerForTest.newDataSource(
                "default?option1=one%20two&option2=y");
        MidasConnectionImpl connection = (MidasConnectionImpl) dataSource.getConnection();
        final String sql = "SELECT currentDatabase()";

        connection.setCatalog("system");
        Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        connection.setCatalog("default");
        ResultSet resultSet = statement.executeQuery(sql);
        resultSet.next();
        assertEquals(resultSet.getString(1), "system");
        assertEquals(resultSet.getType(), ResultSet.TYPE_FORWARD_ONLY);

        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        resultSet = statement.executeQuery(sql);
        resultSet.next();
        assertEquals(resultSet.getString(1), "default");
        assertEquals(resultSet.getType(), ResultSet.TYPE_SCROLL_INSENSITIVE);
        resultSet.beforeFirst();
        resultSet.next();
        assertEquals(resultSet.getString(1), "default");
    }

    @Test
    public void testCreateArrayOf() throws Exception {
        // TODO: more
        MidasDataSource dataSource = MidasContainerForTest.newDataSource("default");
        MidasConnectionImpl connection = (MidasConnectionImpl) dataSource.getConnection();
        for (MidasDataType dataType : MidasDataType.values()) {
            if (dataType == MidasDataType.Array) {
                continue;
            }
            Array a = connection.createArrayOf(dataType.name(), new Object[0]);
            assertEquals(a.getBaseType(), dataType.getSqlType());
            assertEquals(a.getBaseTypeName(), dataType.name());
        }
    }
}
