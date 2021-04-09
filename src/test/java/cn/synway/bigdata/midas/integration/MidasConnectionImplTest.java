package cn.synway.bigdata.midas.integration;

import java.sql.Connection;

import javax.sql.DataSource;

import org.testng.annotations.Test;

import cn.synway.bigdata.midas.MidasContainerForTest;
import cn.synway.bigdata.midas.settings.MidasProperties;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class MidasConnectionImplTest {

    @Test
    public void testDefaultEmpty() throws Exception {
        assertSuccess(createDataSource(null, null));
    }

    @Test
    public void testDefaultUserOnly() throws Exception {
        assertSuccess(createDataSource("default", null));
    }

    @Test
    public void testDefaultUserEmptyPassword() throws Exception {
        assertSuccess(createDataSource("default", ""));
    }

    @Test
    public void testDefaultUserPass() throws Exception {
        assertFailure(createDataSource("default", "bar"));
    }

    @Test
    public void testDefaultPass() throws Exception {
        assertFailure(createDataSource(null, "bar"));
    }

    @Test
    public void testFooEmpty() throws Exception {
        assertFailure(createDataSource("foo", null));
    }

    @Test
    public void testFooWrongPass() throws Exception {
        assertFailure(createDataSource("foo", "baz"));
    }

    @Test
    public void testFooPass() throws Exception {
        assertSuccess(createDataSource("foo", "bar"));
    }

    @Test
    public void testFooWrongUser() throws Exception {
        assertFailure(createDataSource("baz", "bar"));
    }

    @Test
    public void testOofNoPassword() throws Exception {
        assertSuccess(createDataSource("oof", null));
    }

    @Test
    public void testOofWrongPassword() throws Exception {
        assertFailure(createDataSource("oof", "baz"));
    }

    private static void assertSuccess(DataSource dataSource) throws Exception {
        Connection connection = dataSource.getConnection();
        assertTrue(connection.createStatement().execute("SELECT 1"));
    }

    private static void assertFailure(DataSource dataSource) throws Exception {
        // grrr, no JDK 1.8
        // assertThrows(SQLException.class, () -> dataSource.getConnection());
        try {
            dataSource.getConnection();
            fail();
        } catch (RuntimeException e) {
            // exppected
        }
    }

    private static DataSource createDataSource(String user, String password) {
        MidasProperties props = new MidasProperties();
        props.setUser(user);
        props.setPassword(password);
        return MidasContainerForTest.newDataSource(props);
    }
}
