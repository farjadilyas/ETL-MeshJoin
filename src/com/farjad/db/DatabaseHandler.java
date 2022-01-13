package com.farjad.db;

import java.sql.*;

public class DatabaseHandler {

    private DatabaseHandler() {}

    public enum DataSource {
        STREAM_SOURCE_CONN, MD_SOURCE_CONN, DW_OUTPUT_CONN
    }

    private static Connection mdSourceConn;
    private static Connection streamSourceConn;
    private static Connection dwOutputConn;

    public static synchronized Connection openConnection(DataSource dataSource) {
        if (dataSource == DataSource.STREAM_SOURCE_CONN) {
            if (streamSourceConn == null) {
                streamSourceConn = openConnection();
            }
            return streamSourceConn;
        } else if (dataSource == DataSource.MD_SOURCE_CONN) {
            if (mdSourceConn == null) {
                mdSourceConn = openConnection();
            }
            return mdSourceConn;
        } else if (dataSource == DataSource.DW_OUTPUT_CONN) {
            if (dwOutputConn == null) {
                dwOutputConn = openConnection();
            }

            // Ensure auto commit is set to true since we are dealing with a stream
            // If the stream is finite, then it would be more efficient to set this to false
            // Since it would enable a batch commit
            try {
                dwOutputConn.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return dwOutputConn;
        }
        return null;
    }

    private static Connection openConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/DWH_PROJECT", "root", "");
            System.out.println("Connection established with Data Source");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;
    }

    public static synchronized void closeConnection(DataSource dataSource) {
        try {
            Connection connectionToBeClosed = null;
            switch (dataSource) {
                case STREAM_SOURCE_CONN:
                    connectionToBeClosed = streamSourceConn;
                    streamSourceConn = null;
                    break;
                case MD_SOURCE_CONN:
                    connectionToBeClosed = mdSourceConn;
                    mdSourceConn = null;
                    break;
                case DW_OUTPUT_CONN:
                    connectionToBeClosed = dwOutputConn;
                    dwOutputConn = null;
                    break;
            }

            if (connectionToBeClosed != null) {
                connectionToBeClosed.close();
            }
            System.out.println("Connection with Data Source closed");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /* Methods to close ResultSet, PreparedStatement, and Connection */
    private static void closeAll(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {

        closeResultSet(resultSet);
        closeStatement(preparedStatement);
        closeConnection(connection);
    }

    private static void closeConnection(Connection connection) {
        try {
            if (connection != null){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void closeStatement(PreparedStatement preparedStatement) {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void closeResultSet(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
