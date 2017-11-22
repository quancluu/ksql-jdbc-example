package org.quancluu.ksql.jdbc.utils;

import com.github.mmolimar.ksql.jdbc.KsqlDriver;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class KsqlUtilsTest {

    final int limit = 20;

    /*
    Property                                         | Value
--------------------------------------------------------------------------
 ksql.transient.prefix                            | transient_
 ksql.command.topic.suffix                        | commands
 commit.interval.ms                               | 2000
 listeners                                        | http://localhost:9098
 bootstrap.servers                                | 10.2.1.86:9092
 ksql.sink.partitions                             | 4
 ksql.statestore.suffix                           | transient_
 ksql.service.id                                  | ksql_
 ksql.sink.replications                           | 1
 cache.max.bytes.buffering                        | 10000000
 ksql.sink.window.change.log.additional.retention | 1000000
 auto.offset.reset                                | latest
 num.stream.threads                               | 4
 ksql.persistent.prefix                           | query_
 application.id                                   | ksql_

     */

    @Test
    public void testKsqlJdbc() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]

        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            Connection con = DriverManager.getConnection(connectionUrl);
            final String tableName = "PAGEVIEWS_REGIONS";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName + " limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testKsqlJdbcPageViewsOrg() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            Connection con = DriverManager.getConnection(connectionUrl);

            final String tableName = "PAGEVIEWS_ORIGINAL";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName;// + " limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKsqlJdbcPageViewsOrgCustomLimit() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            //     properties.put("auto.offset.reset", "earliest");
            properties.put("auto.offset.reset", "latest");

            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "PAGEVIEWS_ORIGINAL";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName + " limit 30";

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKsqlJdbcPageViewsRegionCustomLimit() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            // properties.put("auto.offset.reset", "latest");

            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "PAGEVIEWS_REGIONS";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName;// + " limit 30";

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKsqlJdbcPageViewsFemaleLikeCustomLimit() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]

        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            // properties.put("auto.offset.reset", "latest");

            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "PAGEVIEWS_FEMALE_LIKE_89";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName;// + " limit 30";

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKsqlJdbcPageViewsFemaleCustomLimit() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]

        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            // properties.put("auto.offset.reset", "latest");

            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "PAGEVIEWS_FEMALE";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName;// + " limit 30";

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKsqlJdbcPageViewsOrgMetadata() throws Exception {

        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            Connection con = DriverManager.getConnection(connectionUrl);

            final String tableName = "PAGEVIEWS_ORIGINAL";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName + " limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);
            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKsqlJdbcPageViewsOrgEarliest() throws Exception {

        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "PAGEVIEWS_ORIGINAL";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName;// + " limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);
            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKsqlJdbcUserOrgEarliest() throws Exception {

        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "USERS_ORIGINAL2";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName;// + " limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);
            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testCreateTable() throws Exception {

        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            Connection con = DriverManager.getConnection(connectionUrl, properties);
            System.out.println("Connected ok");

            final String tableName = "USERS_ORIGINAL2";
            System.out.println("ksql jdbc connection established");

            String sqlStatement = "drop table "+ tableName;

            Statement stmt = con.createStatement();
            boolean status = stmt.execute(sqlStatement);
            System.out.println("drop status=" + status);

            sqlStatement = "CREATE TABLE " + tableName +" (registertime bigint, gender varchar, regionid varchar, userid varchar) WITH (kafka_topic='users', value_format='JSON');";// + " limit " + limit;
            status = stmt.execute(sqlStatement);
            System.out.println("create status=" + status);

            Thread.sleep(2000);
            String query = "select * from "+ tableName + " limit " + limit;

            ResultSet rs = stmt.executeQuery(query);
                printResult(con, rs,tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testCreateStream() throws Exception {

        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            Connection con = DriverManager.getConnection(connectionUrl, properties);
            System.out.println("Connected ok");

            final String streamName = "PAGEVIEWS_ORIGINAL2";
            System.out.println("ksql jdbc connection established");

            String sqlStatement = "drop table " + streamName;

            Statement stmt = con.createStatement();
            boolean status = stmt.execute(sqlStatement);
            System.out.println("drop status=" + status);

            sqlStatement = "CREATE STREAM " + streamName+" (viewtime bigint, userid varchar, pageid varchar) WITH (kafka_topic='pageviews', value_format='DELIMITED');";

            status = stmt.execute(sqlStatement);
            System.out.println("create status=" + status);
            Thread.sleep(1000);

            String query = "select * from "+ streamName;// + " limit " + limit;

            ResultSet rs = stmt.executeQuery(query);
            printResult(con, rs,streamName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Test
    public void testShowTable() throws Exception {

        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:8080";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "PAGEVIEWS_ORIGINAL";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "show tables";// + " limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);
            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKsqlJdbcPageViewsFemale() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            Connection con = DriverManager.getConnection(connectionUrl);

            final String tableName = "PAGEVIEWS_FEMALE";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName + " limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);
            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKsqlJdbcPageViewsFemaleAgg() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            Connection con = DriverManager.getConnection(connectionUrl);

            final String tableName = "PAGEVIEWS_FEMALE";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select USERID, sum(ROWTIME) from " + tableName + " group by USERID " + " limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            //    ResultSetMetaData metaData = rs.getMetaData();

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKsqlJdbcPageViewsOrgAgg() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            //   properties.put("auto.offset.reset", "latest");
            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "PAGEVIEWS_ORIGINAL";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select PAGEID, count(USERID) from " + tableName + " group by PAGEID " + " limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            //    ResultSetMetaData metaData = rs.getMetaData();

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testKsqlJdbcPageViewsOrgAggLimit() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");
            //   properties.put("auto.offset.reset", "latest");
            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "PAGEVIEWS_ORIGINAL";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select PAGEID, count(USERID) from " + tableName + " group by PAGEID"; //limit " + limit;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            //    ResultSetMetaData metaData = rs.getMetaData();

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testKsqlJdbcPageViewsOrgCustomLimitEarliest() throws Exception {
        // The URL has the form jdbc:ksql://<ksql-engine>:<port>[?<property1>=<value>,<property2>=<value>...]


        final String connectionUrl = "jdbc:ksql://localhost:9098";
        try {

            DriverManager.registerDriver(new KsqlDriver());
            final Properties properties = new Properties();
            properties.put("auto.offset.reset", "earliest");

            Connection con = DriverManager.getConnection(connectionUrl, properties);

            final String tableName = "PAGEVIEWS_ORIGINAL";
            System.out.println("ksql jdbc connection established");
            String sqlStatement = "select * from " + tableName;

            System.out.println("Connected ok");
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sqlStatement);

            printResult(con, rs, tableName, sqlStatement);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Object getObject(final ResultSet rs, final int index) throws Exception {
        try {
            return rs.getString(index);
        } catch (Exception e) {

        }

        try {
            return rs.getInt(index);
        } catch (Exception e) {

        }

        try {
            return rs.getLong(index);
        } catch (Exception e) {

        }

        return null;
    }

    private void printResult(final Connection connection, final ResultSet rs, final String tableName, final String sql) throws Exception {
        final long start = System.currentTimeMillis();
        final ResultSet metadataRs = connection.getMetaData().getColumns("", "", tableName, "");
        final Map<String, String> mapData = new HashMap<>();
        Integer count = 0;
        System.out.println("SQL: " + sql);
        System.out.println("\n== Begin Query Results ======================");

        while (metadataRs.next()) {
            //     final String tableName2 = metadataRs.getString("TABLE_NAME");
            final String columnName = metadataRs.getString("COLUMN_NAME");
            if (columnName.startsWith("_")) {
                // Ignore.
            } else {
                count++;
                final int dataType = metadataRs.getInt("DATA_TYPE");
                final String typeName = metadataRs.getString("TYPE_NAME");
                //     System.out.println(columnName);
                mapData.put(count.toString(), columnName);
            }
        }

        final int size = mapData.size();
        System.out.print("Row # | ");
        for (int i = 1; i <= size; i++) {
            System.out.print(mapData.get(Integer.toString(i)) + " | ");
        }
        System.out.println("");

        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            System.out.print(rowCount + " of " + limit + " | ");
            for (int i = 1; i <= size; i++) {
                final Object value = getObject(rs, i);
                System.out.print(value + " | ");
            }
            System.out.println("");
        }

        final long timeTook = System.currentTimeMillis() - start;
        System.out.println("Done printing result set. Time took (msecs): " + timeTook);

    }
}