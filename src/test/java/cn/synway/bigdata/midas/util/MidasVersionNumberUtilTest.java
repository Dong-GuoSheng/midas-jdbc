package cn.synway.bigdata.midas.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MidasVersionNumberUtilTest {

    @Test
    public void testMajorNull() {
        try {
            MidasVersionNumberUtil.getMajorVersion(null);
            Assert.fail();
        } catch (NullPointerException npe) { /* expected */ }
    }

    @Test
    public void testMinorNull() {
        try {
            MidasVersionNumberUtil.getMinorVersion(null);
            Assert.fail();
        } catch (NullPointerException npe) { /* expected */ }
    }

    @Test
    public void testMajorGarbage() {
        Assert.assertEquals(0, MidasVersionNumberUtil.getMajorVersion(""));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMajorVersion("  \t"));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMajorVersion("  \n "));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMajorVersion("."));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMajorVersion(". . "));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMajorVersion("F.O.O"));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMajorVersion("42.foo"));
    }

    @Test
    public void testMajorSimple() {
        Assert.assertEquals(MidasVersionNumberUtil.getMajorVersion("1.0"), 1);
        Assert.assertEquals(MidasVersionNumberUtil.getMajorVersion("1.0.42"), 1);
        Assert.assertEquals(MidasVersionNumberUtil.getMajorVersion("23.42"), 23);
        Assert.assertEquals(MidasVersionNumberUtil.getMajorVersion("1.0.foo"), 1);
        Assert.assertEquals(MidasVersionNumberUtil.getMajorVersion("   1.0"), 1);
        Assert.assertEquals(MidasVersionNumberUtil.getMajorVersion("1.0-SNAPSHOT"), 1);
    }

    @Test
    public void testMinorGarbage() {
        Assert.assertEquals(0, MidasVersionNumberUtil.getMinorVersion(""));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMinorVersion("  \t"));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMinorVersion("  \n "));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMinorVersion("."));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMinorVersion(". . "));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMinorVersion("F.O.O"));
        Assert.assertEquals(0, MidasVersionNumberUtil.getMinorVersion("42.foo"));
    }

    @Test
    public void testMinorSimple() {
        Assert.assertEquals(MidasVersionNumberUtil.getMinorVersion("0.1"), 1);
        Assert.assertEquals(MidasVersionNumberUtil.getMinorVersion("42.1.42"), 1);
        Assert.assertEquals(MidasVersionNumberUtil.getMinorVersion("1.42.foo"), 42);
        Assert.assertEquals(MidasVersionNumberUtil.getMinorVersion("   1.1"), 1);
        Assert.assertEquals(MidasVersionNumberUtil.getMinorVersion("1.1-SNAPSHOT"), 1);
    }

}
