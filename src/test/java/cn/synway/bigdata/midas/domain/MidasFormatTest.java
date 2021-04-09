package cn.synway.bigdata.midas.domain;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MidasFormatTest {

    @Test
    public void testNull() {
        assertFalse(MidasFormat.containsFormat(null));
    }

    @Test
    public void testEmpty() {
        assertFalse(MidasFormat.containsFormat(" \t \r\n"));
    }

    @Test
    public void testTrailingWhitespace() {
        assertFalse(MidasFormat.containsFormat("Phantasy  "));
        assertTrue(MidasFormat.containsFormat("TabSeparatedWithNamesAndTypes "));
        assertTrue(MidasFormat.containsFormat("TabSeparatedWithNamesAndTypes \t \n"));
    }

    @Test
    public void testTrailingSemicolon() {
        assertFalse(MidasFormat.containsFormat("Phantasy  ;"));
        assertTrue(MidasFormat.containsFormat("TabSeparatedWithNamesAndTypes ; "));
        assertTrue(MidasFormat.containsFormat("TabSeparatedWithNamesAndTypes ;"));
        assertTrue(MidasFormat.containsFormat("TabSeparatedWithNamesAndTypes \t ; \n"));
    }

    @Test
    public void testAllFormats() {
        for (MidasFormat format : MidasFormat.values()) {
            assertTrue(MidasFormat.containsFormat(format.name()));
        }
    }

}
