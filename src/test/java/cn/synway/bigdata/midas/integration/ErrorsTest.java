package cn.synway.bigdata.midas.integration;

import com.google.common.base.Throwables;
import org.testng.Assert;
import org.testng.annotations.Test;

import cn.synway.bigdata.midas.MidasContainerForTest;
import cn.synway.bigdata.midas.except.MidasException;
import cn.synway.bigdata.midas.settings.MidasProperties;
import cn.synway.bigdata.midas.util.MidasVersionNumberUtil;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class ErrorsTest {

    @Test
    public void testWrongUser() {
        MidasProperties properties = new MidasProperties();
        properties.setUser("not_existing");
        DataSource dataSource = MidasContainerForTest.newDataSource(properties);
        try {
            Connection connection = dataSource.getConnection();
        } catch (Exception e) {
            String version = MidasContainerForTest.getMidasVersion();
            if (!version.isEmpty() && MidasVersionNumberUtil.getMajorVersion(version) <= 19) {
                Assert.assertEquals((getMidasException(e)).getErrorCode(), 192);
            } else {
                Assert.assertEquals((getMidasException(e)).getErrorCode(), 516);
            }
            return;
        }
        Assert.assertTrue(false, "didn' find correct error");
    }

    @Test(expectedExceptions = MidasException.class)
    public void testTableNotExists() throws SQLException {
        DataSource dataSource = MidasContainerForTest.newDataSource();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("select * from table_not_exists");
    }

    @Test
    public void testErrorDecompression() throws Exception {
        MidasProperties properties = new MidasProperties();
        properties.setCompress(true);
        String[] address = MidasContainerForTest.getMidasHttpAddress().split(":");
        DataSource dataSource = MidasContainerForTest.newDataSource(properties);
        Connection connection = dataSource.getConnection();

        connection.createStatement().execute("DROP TABLE IF EXISTS test.table_not_exists");

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.table_not_exists (d, s) VALUES (?, ?)");

        statement.setDate(1, new Date(System.currentTimeMillis()));
        statement.setInt(2, 1);
        try {
            statement.executeBatch();
        } catch (Exception e) {
            String exceptionMsg = getMidasException(e).getMessage();
            Assert.assertTrue(exceptionMsg.startsWith("Midas exception, code: 60, host: " + address[0] +", port: " + address[1] +"; Code: 60, e.displayText() = DB::Exception: Table test.table_not_exists doesn't exist"), exceptionMsg);
            return;
        }
        Assert.assertTrue(false, "didn' find correct error");
    }

    private static MidasException getMidasException(Exception e) {
        List<Throwable> causalChain = Throwables.getCausalChain(e);
        for (Throwable throwable : causalChain) {
            if (throwable instanceof MidasException) {
                return (MidasException) throwable;
            }
        }
        throw new IllegalArgumentException("no MidasException found");
    }
}
