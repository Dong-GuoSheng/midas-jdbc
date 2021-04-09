package cn.synway.bigdata.midas.jdbc.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MidasSqlUtilsTest {
    @Test
    public void testIsQuote() {
        Assert.assertFalse(MidasSqlUtils.isQuote('\0'));

        Assert.assertTrue(MidasSqlUtils.isQuote('"'));
        Assert.assertTrue(MidasSqlUtils.isQuote('\''));
        Assert.assertTrue(MidasSqlUtils.isQuote('`'));
    }

    @Test
    public void testEscape() {
        char[] quotes = new char[] { '"', '\'', '`' };
        String str;
        for (int i = 0; i < quotes.length; i++) {
            char quote = quotes[i];
            Assert.assertEquals(MidasSqlUtils.escape(str = null, quote), str);
            Assert.assertEquals(MidasSqlUtils.escape(str = "", quote),
                    String.valueOf(quote) + String.valueOf(quote));
            Assert.assertEquals(MidasSqlUtils.escape(str = "\\any \\string\\", quote),
                    String.valueOf(quote) + "\\\\any \\\\string\\\\" + String.valueOf(quote));
            Assert.assertEquals(
                    MidasSqlUtils.escape(str = String.valueOf(quote) + "any " + String.valueOf(quote) + "string",
                            quote),
                    String.valueOf(quote) + "\\" + String.valueOf(quote) + "any \\" + String.valueOf(quote) + "string"
                            + String.valueOf(quote));
            Assert.assertEquals(MidasSqlUtils.escape(str = "\\any \\string\\" + String.valueOf(quote), quote),
                    String.valueOf(quote) + "\\\\any \\\\string\\\\\\" + String.valueOf(quote) + String.valueOf(quote));
            Assert.assertEquals(
                    MidasSqlUtils.escape(str = String.valueOf(quote) + "\\any \\" + String.valueOf(quote)
                            + "string\\" + String.valueOf(quote), quote),
                    String.valueOf(quote) + "\\" + String.valueOf(quote) + "\\\\any \\\\\\" + String.valueOf(quote)
                            + "string" + "\\\\\\" + String.valueOf(quote) + String.valueOf(quote));
        }
    }

    @Test
    public void testUnescape() {
        String str;
        Assert.assertEquals(MidasSqlUtils.unescape(str = null), str);
        Assert.assertEquals(MidasSqlUtils.unescape(str = ""), str);
        Assert.assertEquals(MidasSqlUtils.unescape(str = "\\any \\string\\"), str);
        char[] quotes = new char[] { '"', '\'', '`' };
        for (int i = 0; i < quotes.length; i++) {
            char quote = quotes[i];
            Assert.assertEquals(MidasSqlUtils.unescape(str = String.valueOf(quote) + "1" + String.valueOf(quote)),
                    "1");
            Assert.assertEquals(MidasSqlUtils.unescape(str = String.valueOf(quote) + "\\any \\string\\"), str);
            Assert.assertEquals(MidasSqlUtils.unescape(str = "\\any \\string\\" + String.valueOf(quote)), str);
            Assert.assertEquals(
                    MidasSqlUtils.unescape(str = String.valueOf(quote) + "\\any" + String.valueOf(quote)
                            + String.valueOf(quote) + "\\string\\" + String.valueOf(quote)),
                    "any" + String.valueOf(quote) + "string\\");
            Assert.assertEquals(
                    MidasSqlUtils.unescape(str = String.valueOf(quote) + String.valueOf(quote) + "\\"
                            + String.valueOf(quote) + "any" + String.valueOf(quote) + String.valueOf(quote)
                            + "\\string\\" + String.valueOf(quote)),
                    String.valueOf(quote) + String.valueOf(quote) + "any" + String.valueOf(quote) + "string\\");
        }
    }
}
