package cn.synway.bigdata.midas;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import cn.synway.bigdata.midas.settings.MidasProperties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class BalancedMidasDataSourceTest {

    private BalancedMidasDataSource dataSource;
    private BalancedMidasDataSource doubleDataSource;

    @Test
    public void testUrlSplit() throws Exception {
        assertEquals(Arrays.asList("jdbc:midas://localhost:1234/ppc"),
                BalancedMidasDataSource.splitUrl("jdbc:midas://localhost:1234/ppc"));

        assertEquals(Arrays.asList("jdbc:midas://localhost:1234/ppc",
                "jdbc:midas://another.host.com:4321/ppc"),
                BalancedMidasDataSource.splitUrl("jdbc:midas://localhost:1234,another.host.com:4321/ppc"));

        assertEquals(Arrays.asList("jdbc:midas://localhost:1234", "jdbc:midas://another.host.com:4321"),
                BalancedMidasDataSource.splitUrl("jdbc:midas://localhost:1234,another.host.com:4321"));

    }


    @Test
    public void testUrlSplitValidHostName() throws Exception {
        assertEquals(Arrays.asList("jdbc:midas://localhost:1234", "jdbc:midas://_0another-host.com:4321"),
                BalancedMidasDataSource.splitUrl("jdbc:midas://localhost:1234,_0another-host.com:4321"));

    }


    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUrlSplitInvalidHostName() throws Exception {
        BalancedMidasDataSource.splitUrl("jdbc:midas://localhost:1234,_0ano^ther-host.com:4321");

    }

    @BeforeTest
    public void setUp() throws Exception {
        dataSource = MidasContainerForTest.newBalancedDataSource();
        String address = MidasContainerForTest.getMidasHttpAddress();
        doubleDataSource = MidasContainerForTest.newBalancedDataSource(address, address);
    }

    @Test
    public void testSingleDatabaseConnection() throws Exception {
        Connection connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");

        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.insert_test (i Int32, s String) ENGINE = TinyLog"
        );
        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();


        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));
    }

    @Test
    public void testDoubleDatabaseConnection() throws Exception {
        Connection connection = doubleDataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
        connection = doubleDataSource.getConnection();
        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.insert_test (i Int32, s String) ENGINE = TinyLog"
        );

        connection = doubleDataSource.getConnection();

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));

        connection = doubleDataSource.getConnection();

        statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();

        rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));

    }

    @Test
    public void testCorrectActualizationDatabaseConnection() throws Exception {
        dataSource.actualize();
        Connection connection = dataSource.getConnection();
    }


    @Test
    public void testDisableConnection() throws Exception {
        BalancedMidasDataSource badDatasource = MidasContainerForTest.newBalancedDataSource("not.existed.url:8123");
        badDatasource.actualize();
        try {
            Connection connection = badDatasource.getConnection();
            fail();
        } catch (Exception e) {
            // There is no enabled connections
        }
    }


    @Test
    public void testWorkWithEnabledUrl() throws Exception {
        BalancedMidasDataSource halfDatasource = MidasContainerForTest.newBalancedDataSource("not.existed.url:8123", MidasContainerForTest.getMidasHttpAddress());

        halfDatasource.actualize();
        Connection connection = halfDatasource.getConnection();

        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
        connection = halfDatasource.getConnection();
        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert_test");
        connection.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS test.insert_test (i Int32, s String) ENGINE = TinyLog"
        );

        connection = halfDatasource.getConnection();

        PreparedStatement statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();

        ResultSet rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));

        connection = halfDatasource.getConnection();

        statement = connection.prepareStatement("INSERT INTO test.insert_test (s, i) VALUES (?, ?)");
        statement.setString(1, "asd");
        statement.setInt(2, 42);
        statement.execute();

        rs = connection.createStatement().executeQuery("SELECT * from test.insert_test");
        rs.next();

        assertEquals("asd", rs.getString("s"));
        assertEquals(42, rs.getInt("i"));
    }

    @Test
    public void testConstructWithMidasProperties() {
        final MidasProperties properties = new MidasProperties();
        properties.setMaxThreads(3);
        properties.setSocketTimeout(67890);
        properties.setPassword("888888");
        //without connection parameters
        String hostAddr = MidasContainerForTest.getMidasHttpAddress();
        String ipAddr   = MidasContainerForTest.getMidasHttpAddress(true);
        BalancedMidasDataSource dataSource = MidasContainerForTest.newBalancedDataSourceWithSuffix(
            "click", properties, hostAddr, ipAddr);
        MidasProperties dataSourceProperties = dataSource.getProperties();
        assertEquals(dataSourceProperties.getMaxThreads().intValue(), 3);
        assertEquals(dataSourceProperties.getSocketTimeout(), 67890);
        assertEquals(dataSourceProperties.getPassword(), "888888");
        assertEquals(dataSourceProperties.getDatabase(), "click");
        assertEquals(2, dataSource.getAllMidasUrls().size());
        assertEquals(dataSource.getAllMidasUrls().get(0), "jdbc:midas://" + hostAddr + "/click");
        assertEquals(dataSource.getAllMidasUrls().get(1), "jdbc:midas://" + ipAddr + "/click");
        // with connection parameters
        dataSource = MidasContainerForTest.newBalancedDataSourceWithSuffix(
                "click?socket_timeout=12345&user=readonly", properties, hostAddr, ipAddr);
        dataSourceProperties = dataSource.getProperties();
        assertEquals(dataSourceProperties.getMaxThreads().intValue(), 3);
        assertEquals(dataSourceProperties.getSocketTimeout(), 12345);
        assertEquals(dataSourceProperties.getUser(), "readonly");
        assertEquals(dataSourceProperties.getPassword(), "888888");
        assertEquals(dataSourceProperties.getDatabase(), "click");
        assertEquals(2, dataSource.getAllMidasUrls().size());
        assertEquals(dataSource.getAllMidasUrls().get(0), "jdbc:midas://" + hostAddr + "/click?socket_timeout" +
                "=12345&user=readonly");
        assertEquals(dataSource.getAllMidasUrls().get(1), "jdbc:midas://" + ipAddr + "/click?socket_timeout=12345&user=readonly");
    }

    @Test
    public void testConnectionWithAuth() throws SQLException {
        final MidasProperties properties = new MidasProperties();
        final String hostAddr = MidasContainerForTest.getMidasHttpAddress();
        final String ipAddr = MidasContainerForTest.getMidasHttpAddress(true);

        final BalancedMidasDataSource dataSource0 = MidasContainerForTest
            .newBalancedDataSourceWithSuffix(
                "default?user=foo&password=bar",
                properties,
                hostAddr,
                ipAddr);
        assertTrue(dataSource0.getConnection().createStatement().execute("SELECT 1"));

        final BalancedMidasDataSource dataSource1 = MidasContainerForTest
            .newBalancedDataSourceWithSuffix(
                "default?user=foo",
                properties,
                hostAddr,
                ipAddr);
        // assertThrows(RuntimeException.class,
        //    () -> dataSource1.getConnection().createStatement().execute("SELECT 1"));


        final BalancedMidasDataSource dataSource2 = MidasContainerForTest
            .newBalancedDataSourceWithSuffix(
                "default?user=oof",
                properties,
                hostAddr,
                ipAddr);
        assertTrue(dataSource2.getConnection().createStatement().execute("SELECT 1"));

        properties.setUser("foo");
        properties.setPassword("bar");
        final BalancedMidasDataSource dataSource3 = MidasContainerForTest
            .newBalancedDataSourceWithSuffix(
                "default",
                properties,
                hostAddr,
                ipAddr);
        assertTrue(dataSource3.getConnection().createStatement().execute("SELECT 1"));

        properties.setPassword("bar");
        final BalancedMidasDataSource dataSource4 = MidasContainerForTest
            .newBalancedDataSourceWithSuffix(
                "default?user=oof",
                properties,
                hostAddr,
                ipAddr);
        // JDK 1.8
        // assertThrows(RuntimeException.class,
        //    () -> dataSource4.getConnection().createStatement().execute("SELECT 1"));
        try {
            dataSource4.getConnection().createStatement().execute("SELECT 1");
            fail();
        } catch (RuntimeException re) {
            // expected
        }

        // it is not allowed to have query parameters per host
        try {
            MidasContainerForTest
            .newBalancedDataSourceWithSuffix(
                "default?user=oof",
                properties,
                hostAddr + "/default?user=foo&password=bar",
                ipAddr);
            fail();
        } catch (IllegalArgumentException iae) {
            // expected
        }

        // the following behavior is quite unexpected, honestly
        // but query params always have precedence over properties
        final BalancedMidasDataSource dataSource5 = MidasContainerForTest
            .newBalancedDataSourceWithSuffix(
                "default?user=foo&password=bar",
                properties,
                hostAddr,
                ipAddr);
        assertTrue(
            dataSource5.getConnection("broken", "hacker").createStatement().execute("SELECT 1"));

        // now the other way round, also strange
        final BalancedMidasDataSource dataSource6 = MidasContainerForTest
            .newBalancedDataSourceWithSuffix(
                "default?user=broken&password=hacker",
                properties,
                hostAddr,
                ipAddr);
        // JDK 1.8
        // assertThrows(RuntimeException.class,
        //    () -> dataSource6.getConnection("foo", "bar").createStatement().execute("SELECT 1"));
        try {
            dataSource6.getConnection("foo", "bar").createStatement().execute("SELECT 1");
            fail();
        } catch (RuntimeException re) {
            // expected
        }
    }

}
