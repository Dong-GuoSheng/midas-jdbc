package cn.synway.bigdata.midas.integration;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import cn.synway.bigdata.midas.MidasContainerForTest;
import cn.synway.bigdata.midas.MidasDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Here it is assumed the connection to a Midas instance with flights example data it available at localhost:8123
 * For Midas quickstart and example dataset see <a href="https://Midas.yandex/tutorial.html">https://Midas.yandex/tutorial.html</a>
 */
public class OnTime {

    private MidasDataSource dataSource;
    private Connection connection;

    @BeforeTest
    public void setUp() throws Exception {
        dataSource = MidasContainerForTest.newDataSource();
        connection = dataSource.getConnection();
        connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS test");
    }

    @Test(enabled = false)
    public void simpleSelect() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("select FlightDate, AirlineID, FlightNum from ontime limit 10");
        while (rs.next()) {
            System.out.println(rs.getDate("FlightDate") + ";" + rs.getInt("AirlineID") + ";" + rs.getString("FlightNum"));
        }
        statement.close();
    }

    @Test(enabled = false)
    public void mostTrendingDestinationTest() throws SQLException {
        String query =
            "SELECT \n" +
                "    DestCityName, \n" +
                "    sum(Year = 2014) AS c2014, \n" +
                "    sum(Year = 2015) AS c2015, \n" +
                "    c2015 / c2014 AS diff\n" +
                "FROM ontime\n" +
                "WHERE Year IN (2014, 2015)\n" +
                "GROUP BY DestCityName\n" +
                "HAVING (c2014 > 10000) AND (c2015 > 1000) AND (diff > 1)\n" +
                "ORDER BY diff DESC";

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query);
        while (rs.next()) {
            System.out.println(rs.getString("DestCityName") + ";" + rs.getBigDecimal("c2014") + ";" + rs.getBigDecimal("c2015") + ";" + rs.getFloat("diff"));
        }
        statement.close();
    }

}
