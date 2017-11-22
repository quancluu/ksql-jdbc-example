/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package org.quancluu.ksql.jdbc.utils;

import com.github.mmolimar.ksql.jdbc.KsqlDriver;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class KsqlUtils {
    static final private int limit = 0;
    static private Object getObject(final ResultSet rs, final int index) throws Exception{
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

    static private void printResult(final Connection connection, final ResultSet rs, final String tableName, final String sql) throws Exception {
        final long start = System.currentTimeMillis();
        final ResultSet metadataRs = connection.getMetaData().getColumns("", "", tableName, "");
        final Map<String, String> mapData = new HashMap<>();
        Integer count=0;
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
    static public void connect(final String connectionUrl, final String jdbcDriverName, String query) throws Exception {

        String sqlStatement = query; // "show tables";
        System.out.println("\n=============================================");
        System.out.println("Cloudera KSQL JDBC Example");
        System.out.println("Using Connection URL: " + connectionUrl);
        System.out.println("Running Query: " + sqlStatement);

        Connection con = null;

        try {

            Class.forName(jdbcDriverName);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            try {

                DriverManager.registerDriver(new KsqlDriver());
                con = DriverManager.getConnection(connectionUrl);
                final String tableName = "PAGEVIEWS_REGIONS";
                System.out.println("ksql jdbc connection established");

                System.out.println("Connected ok");
                Statement stmt = con.createStatement();

                ResultSet rs = stmt.executeQuery(sqlStatement);

                printResult(con, rs, tableName, sqlStatement);

            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (Exception e) {
                // swallow
            }
        }
    }

    // Usage:  java com.zoomdata.fabric.tools.impala.ImpalaUtils "jdbc:hive2://10.0.100.113:21050/default;auth=noSasl" "show tables");
    public static void main(String[] args) throws Exception {
        final String jdbcDriverName = "org.apache.hive.jdbc.HiveDriver";
        String connectionUrl = "jdbc:ksql://localhost:9098";


        System.out.println("Usage:  com.zoomdata.fabric.tools.impala.ImpalaUtils \"jdbc:hive2://10.0.100.113:21050/default;auth=noSasl\" \"show tables\"");
        System.out.println("# args=" + args.length);

        String query = "select * from PAGEVIEWS_ORIGINAL limit 1000";
        if (args.length > 0) {
            connectionUrl = args[0];
            if (args.length > 1) {
                query = args[1];
            }
        } else {

        }
        System.out.println("Connection URL: " + connectionUrl + ", driver class: " + jdbcDriverName);

        connect(connectionUrl, jdbcDriverName, query);

    }
}
