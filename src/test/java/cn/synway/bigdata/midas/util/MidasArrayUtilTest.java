package cn.synway.bigdata.midas.util;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class MidasArrayUtilTest {

    @Test
    public void testArrayToString() throws Exception {
        assertEquals(
            MidasArrayUtil.arrayToString(new String[]{"a", "b"}),
            "['a','b']"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new String[]{"a", "'b\t"}),
            "['a','\\'b\\t']"
        );

        assertEquals(
                MidasArrayUtil.arrayToString(new String[]{"\\xEF\\xBC", "\\x3C\\x22"}), // quote == true
                "['\\\\xEF\\\\xBC','\\\\x3C\\\\x22']"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new Integer[]{21, 42}),
            "[21,42]"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new int[]{21, 42}),
            "[21,42]"
        );

        assertEquals(
                MidasArrayUtil.arrayToString(new double[]{0.1, 1.2}),
                "[0.1,1.2]"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new char[]{'a', 'b'}),
            "['a','b']"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new String[][]{{"a", "b"},{"c", "d"}}),
            "[['a','b'],['c','d']]"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new String[][]{{"a", "'b\t"},{"c", "'d\t"}}),
            "[['a','\\'b\\t'],['c','\\'d\\t']]"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new Integer[][]{{21, 42},{63, 84}}),
            "[[21,42],[63,84]]"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new double[][]{{0.1, 1.2}, {0.2, 2.2}}),
            "[[0.1,1.2],[0.2,2.2]]"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new int[][]{{1, 2}, {3, 4}}),
            "[[1,2],[3,4]]"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new char[][]{{'a', 'b'}, {'c', 'd'}}),
            "[['a','b'],['c','d']]"
        );

        assertEquals(
            MidasArrayUtil.arrayToString(new byte[][]{{'a', 'b'}, {'c', 'd'}}),
            "['\\x61\\x62','\\x63\\x64']"
        );

    }

    @Test
    public void testCollectionToString() throws Exception {
        assertEquals(
                MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList("a", "b"))),
                "['a','b']"
        );

        assertEquals(
                MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList("a", "'b\t"))),
                "['a','\\'b\\t']"
        );

        assertEquals(
                MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList("\\xEF\\xBC", "\\x3C\\x22"))), // quote == true
                "['\\\\xEF\\\\xBC','\\\\x3C\\\\x22']"
        );

        assertEquals(
                MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList(21, 42))),
                "[21,42]"
        );

        assertEquals(
                MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList(21, 42))),
                "[21,42]"
        );

        assertEquals(
                MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList(0.1, 1.2))),
                "[0.1,1.2]"
        );

        assertEquals(
                MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList('a', 'b'))),
                "['a','b']"
        );

        ArrayList<Object> arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(1, 2)));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(3, 4)));
        assertEquals(
            MidasArrayUtil.toString(arrayOfArrays),
            "[[1,2],[3,4]]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(1.1, 2.4)));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(3.9, 4.16)));
        assertEquals(
            MidasArrayUtil.toString(arrayOfArrays),
            "[[1.1,2.4],[3.9,4.16]]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList("a", "b")));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList("c", "'d\t")));
        assertEquals(
            MidasArrayUtil.toString(arrayOfArrays),
            "[['a','b'],['c','\\'d\\t']]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('a', 'b')));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('c', 'd')));
        assertEquals(
            MidasArrayUtil.toString(arrayOfArrays),
            "[['a','b'],['c','d']]"
        );

        assertEquals(
            MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList(21, null))),
            "[21,NULL]"
        );

        assertEquals(
            MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList(null, 42))),
            "[NULL,42]"
        );

        assertEquals(
            MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList("a", null))),
            "['a',NULL]"
        );

        assertEquals(
            MidasArrayUtil.toString(new ArrayList<Object>(Arrays.asList(null, "b"))),
            "[NULL,'b']"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(null, 'b')));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('c', 'd')));
        assertEquals(
            MidasArrayUtil.toString(arrayOfArrays),
            "[[NULL,'b'],['c','d']]"
        );

        arrayOfArrays = new ArrayList<Object>();
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList(null, 'b')));
        arrayOfArrays.add(new ArrayList<Object>(Arrays.asList('c', null)));
        assertEquals(
            MidasArrayUtil.toString(arrayOfArrays),
            "[[NULL,'b'],['c',NULL]]"
        );

        List<byte[]> listOfByteArrays = new ArrayList<byte[]>();
        listOfByteArrays.add("foo".getBytes("UTF-8"));
        listOfByteArrays.add("bar".getBytes("UTF-8"));
        assertEquals(
            MidasArrayUtil.toString(listOfByteArrays),
            "['\\x66\\x6F\\x6F','\\x62\\x61\\x72']"
        );
    }

    @Test
    public void testArrayDateTimeDefaultTimeZone() {
        Timestamp ts0 = new Timestamp(1557136800000L);
        Timestamp ts1 = new Timestamp(1560698526598L);
        Timestamp[] timestamps = new Timestamp[] { ts0, null, ts1 };
        String formatted = MidasArrayUtil.arrayToString(timestamps);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        assertEquals(
            formatted,
            "['" + sdf.format(ts0) + "',NULL,'" + sdf.format(ts1) + "']");
    }

    @Test
    public void testArrayDateTimeOtherTimeZone() {
        TimeZone tzTokyo = TimeZone.getTimeZone("Asia/Tokyo");
        Timestamp ts0 = new Timestamp(1557136800000L);
        Timestamp ts1 = new Timestamp(1560698526598L);
        Timestamp[] timestamps = new Timestamp[] { ts0, null, ts1 };
        String formatted = MidasArrayUtil.arrayToString(
            timestamps, tzTokyo, tzTokyo);
        assertEquals(
            formatted,
            "['2019-05-06 19:00:00',NULL,'2019-06-17 00:22:06']");
    }

}
