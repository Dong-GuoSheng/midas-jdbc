package cn.synway.bigdata.midas.integration;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import cn.synway.bigdata.midas.settings.MidasQueryParam;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertTrue;

public class ResultSummaryTest {
    private MidasConnection connection;

    @BeforeTest
    public void setUp() throws Exception {
        connection = MidasContainerForTest.newDataSource().getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @AfterTest
    public void tearDown() throws Exception {
        connection.createStatement().execute("DROP DATABASE IF EXISTS test");
    }

    @Test
    public void select() throws Exception {
        MidasStatement st = connection.createStatement();
        st.executeQuery("SELECT * FROM numbers(10)", Collections.singletonMap(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertTrue(st.getResponseSummary().getReadRows() >= 10);
        assertTrue(st.getResponseSummary().getReadBytes() > 0);
    }

    @Test
    public void largeSelect() throws Exception {
        MidasStatement st = connection.createStatement();
        st.executeQuery("SELECT * FROM numbers(10000000)", Collections.singletonMap(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertTrue(st.getResponseSummary().getReadRows() < 10000000);
        assertTrue(st.getResponseSummary().getReadBytes() > 0);
    }

    @Test
    public void largeSelectWaitEndOfQuery() throws Exception {
        MidasStatement st = connection.createStatement();
        st.executeQuery("SELECT * FROM numbers(10000000)", largeSelectWaitEndOfQueryParams());

        assertTrue(st.getResponseSummary().getReadRows() >= 10000000);
        assertTrue(st.getResponseSummary().getReadBytes() > 0);
    }

    private Map<MidasQueryParam, String> largeSelectWaitEndOfQueryParams() {
        Map<MidasQueryParam, String> res = new HashMap<>();
        res.put(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true");
        res.put(MidasQueryParam.WAIT_END_OF_QUERY, "true");
        return res;
    }

    @Test
    public void selectWithoutParam() throws Exception {
        MidasStatement st = connection.createStatement();
        st.executeQuery("SELECT * FROM numbers(10)", Collections.singletonMap(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertTrue(st.getResponseSummary().getReadRows() >= 10);
        assertTrue(st.getResponseSummary().getReadBytes() > 0);
    }

    @Test
    public void insertSingle() throws Exception {
        createInsertTestTable();

        MidasPreparedStatement ps = (MidasPreparedStatement) connection.prepareStatement("INSERT INTO test.insert_test VALUES(?)");
        ps.setLong(1, 1);
        ps.executeQuery(Collections.singletonMap(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertEquals(ps.getResponseSummary().getWrittenRows(), 1);
        assertTrue(ps.getResponseSummary().getWrittenBytes() > 0);
    }

    @Test
    public void insertBatch() throws Exception {
        createInsertTestTable();

        MidasPreparedStatement ps = (MidasPreparedStatement) connection.prepareStatement("INSERT INTO test.insert_test VALUES(?)");
        for (long i = 0; i < 10; i++) {
            ps.setLong(1, i);
            ps.addBatch();
        }
        ps.executeBatch(Collections.singletonMap(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertEquals(ps.getResponseSummary().getWrittenRows(), 10);
        assertTrue(ps.getResponseSummary().getWrittenBytes() > 0);
    }

    @Test
    public void insertSelect() throws Exception {
        createInsertTestTable();

        MidasPreparedStatement ps = (MidasPreparedStatement) connection.prepareStatement("INSERT INTO test.insert_test SELECT number FROM numbers(10)");
        ps.executeQuery(Collections.singletonMap(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertEquals(ps.getResponseSummary().getWrittenRows(), 10);
        assertTrue(ps.getResponseSummary().getWrittenBytes() > 0);
    }

    @Test
    public void insertLargeSelect() throws Exception {
        createInsertTestTable();

        MidasPreparedStatement ps = (MidasPreparedStatement) connection.prepareStatement("INSERT INTO test.insert_test SELECT number FROM numbers(10000000)");
        ps.executeQuery(Collections.singletonMap(MidasQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, "true"));

        assertEquals(ps.getResponseSummary().getWrittenRows(), 10000000);
        assertTrue(ps.getResponseSummary().getWrittenBytes() > 0);
    }

    @Test
    public void noSummary() throws Exception {
        MidasStatement st = connection.createStatement();
        st.executeQuery("SELECT * FROM numbers(10)");

        assertNull(st.getResponseSummary());
    }

    private void createInsertTestTable() throws SQLException {
        connection.createStatement().execute("DROP TABLE IF EXISTS test.insert_test");
        connection.createStatement().execute("CREATE TABLE IF NOT EXISTS test.insert_test (value UInt32) ENGINE = TinyLog");
    }
}
