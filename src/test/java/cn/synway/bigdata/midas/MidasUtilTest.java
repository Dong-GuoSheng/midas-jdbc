package cn.synway.bigdata.midas;


import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class MidasUtilTest {

    @Test
    public void testQuote() throws Exception {
        assertEquals("\\N", MidasUtil.escape(null));
        assertEquals("test", MidasUtil.escape("test"));
        assertEquals("t\\n\\0\\r\\test\\'", MidasUtil.escape("t\n\0\r\test'"));
    }

    @Test
    public void testQuoteIdentifier() throws Exception {
        assertEquals("`z`", MidasUtil.quoteIdentifier("z"));
        assertEquals("`a\\`\\' `", MidasUtil.quoteIdentifier("a`' "));

        try {
            MidasUtil.quoteIdentifier(null);
            fail("quiteIdentifier with null argument must fail");
        } catch (IllegalArgumentException ex) {
            // pass, it's ok
        }
    }
}