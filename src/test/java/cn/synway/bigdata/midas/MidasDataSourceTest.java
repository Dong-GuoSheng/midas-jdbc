package cn.synway.bigdata.midas;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class MidasDataSourceTest {
    @Test
    public void testConstructor() throws Exception {
        MidasDataSource ds = new MidasDataSource("jdbc:midas://localhost:1234/ppc");
        assertEquals("localhost", ds.getHost());
        assertEquals(1234, ds.getPort());
        assertEquals("ppc", ds.getDatabase());

        MidasDataSource ds2 = new MidasDataSource("jdbc:midas://clh.company.com:5324");
        assertEquals("clh.company.com", ds2.getHost());
        assertEquals(5324, ds2.getPort());
        assertEquals("default", ds2.getDatabase());

        try {
            new MidasDataSource(null);
            fail("MidasDataSource with null argument must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }

        try {
            new MidasDataSource("jdbc:mysql://localhost:2342");
            fail("MidasDataSource with incorrect args must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }

        try {
            new MidasDataSource("jdbc:midas://localhost:wer");
            fail("MidasDataSource with incorrect args must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }
    }

    @Test
    public void testIPv6Constructor() throws Exception {
        MidasDataSource ds = new MidasDataSource("jdbc:midas://[::1]:5324");
        assertEquals(ds.getHost(), "[::1]");
        assertEquals(ds.getPort(), 5324);
        assertEquals(ds.getDatabase(), "default");

        MidasDataSource ds2 = new MidasDataSource("jdbc:midas://[::FFFF:129.144.52.38]:5324");
        assertEquals(ds2.getHost(), "[::FFFF:129.144.52.38]");
        assertEquals(ds2.getPort(), 5324);
        assertEquals(ds2.getDatabase(), "default");
    }

}
