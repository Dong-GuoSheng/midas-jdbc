package cn.synway.bigdata.midas.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import cn.synway.bigdata.midas.MidasContainerForTest;
import cn.synway.bigdata.midas.MidasDataSource;
import cn.synway.bigdata.midas.settings.MidasProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MidasLZ4StreamTest {
    private MidasDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        MidasProperties properties = new MidasProperties();
        properties.setDecompress(true);
        dataSource = MidasContainerForTest.newDataSource(properties);
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test
    public void testBigBatchCompressedInsert() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.big_batch_insert");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.big_batch_insert (i Int32, s String) ENGINE = TinyLog"
        );

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.big_batch_insert (s, i) VALUES (?, ?)");

        int cnt = 1000000;
        for (int i = 0; i < cnt; i++) {
            statement.setString(1, "string" + i);
            statement.setInt(2, i);
            statement.addBatch();
        }

        statement.executeBatch();

        ResultSet rs = connection.createStatement().executeQuery("SELECT count() as cnt from test.big_batch_insert");
        rs.next();
        Assert.assertEquals(rs.getInt("cnt"), cnt);
        Assert.assertFalse(rs.next());
    }
}
