package co.ke.CoreNexus.db_utils.data.generator;

import co.ke.CoreNexus.db_utils.db.connection.DatabaseConnector;
import co.ke.CoreNexus.db_utils.db.utils.Randomizer;

import java.sql.*;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.data.generator)
 * Created by: oloo
 * On: 11/11/2024. 23:50
 * Description:
 **/


public class PrimaryKeyGenerator {

    private static final Connection dbConnection;

    static {
        try {
            dbConnection = DatabaseConnector.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    // Generate the primary key based on column type and auto-increment status
    public static Object generatePrimaryKeyFromColumnName(String tableName, String columnName, String type, int columnSize) throws SQLException {
        boolean isAutoIncrement = isAutoIncrement(tableName, columnName);
        String sequenceName = null;
        
        if (isAutoIncrement || isAutoIncrementType(type)) {
            // Auto-increment or SERIAL fields - let the database handle this
            return "";  // Empty string means the DB will handle it
        }

        // Retrieve the sequence name if the column uses a sequence (like SERIAL in PostgreSQL)
        if (type.equalsIgnoreCase("serial")) {
            sequenceName = getSequenceName(tableName, columnName);
        }

        // Handle INT and LONG types, generating the next value in sequence
        if (type.equalsIgnoreCase("int")) {
            return generateNextInt(tableName, columnName);
        } else if (type.equalsIgnoreCase("long")) {
            return generateNextLong(tableName, columnName);
        } else if (type.equalsIgnoreCase("serial")) {
            return generateNextSerial(sequenceName);
        }

        // If it's not numeric or serial, handle it as a string-based primary key
        return generateStringPrimaryKey(columnName, columnSize);
    }

    // Retrieve the sequence name used by a column (in case of SERIAL, for example)
    private static String getSequenceName(String tableName, String columnName) throws SQLException {
        String query = "SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "' AND COLUMN_NAME = '" + columnName + "'";
        try (Statement stmt = dbConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                String columnDefault = rs.getString("COLUMN_DEFAULT");
                if (columnDefault != null && columnDefault.contains("nextval")) {
                    // Extract the sequence name from the default value
                    int startIdx = columnDefault.indexOf("'") + 1;
                    int endIdx = columnDefault.lastIndexOf("'");
                    return columnDefault.substring(startIdx, endIdx);
                } else {
                    throw new SQLException("No sequence found for the column.");
                }
            } else {
                throw new SQLException("Failed to retrieve sequence name for column.");
            }
        }
    }
    // Check if the column is auto-increment (auto_increment or identity in the database)
    private static boolean isAutoIncrement(String tableName, String columnName) throws SQLException {
        String query = "SELECT EXTRA FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND TABLE_SCHEMA = DATABASE()";

        try (PreparedStatement stmt = dbConnection.prepareStatement(query)) {
            stmt.setString(1, tableName);
            stmt.setString(2, columnName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String extra = rs.getString("EXTRA");
                    return "auto_increment".equalsIgnoreCase(extra);
                } else {
                    throw new SQLException("Failed to retrieve column details for " + columnName);
                }
            }
        }
    }


    // Check if the column is auto-increment or SERIAL
    private static boolean isAutoIncrementType(String type) {
        return type.equalsIgnoreCase("auto_increment") || type.equalsIgnoreCase("serial");
    }

    // Generate the next INT value in sequence (assuming INT is the primary key)
    private static int generateNextInt(String tableName, String columnName) throws SQLException {
        return getLastInsertedInt(tableName, columnName) + 1;
    }

    // Generate the next LONG value in sequence (assuming LONG is the primary key)
    private static long generateNextLong(String tableName, String columnName) throws SQLException {
        return getLastInsertedLong(tableName, columnName) + 1L;
    }

    // Generate the next SERIAL value from the sequence (for databases that support SERIAL, like PostgreSQL)
    private static int generateNextSerial(String sequenceName) throws SQLException {
        return getNextSerialFromDB(sequenceName);
    }

    // Generate a string-based primary key (used when the column is not INT, LONG, or SERIAL)
    private static String generateStringPrimaryKey(String columnName, int columnSize) {
        String normalizedColumnName = columnName.toLowerCase().replaceAll("_", " ");
        String[] words = normalizedColumnName.split("(?<=.)(?=\\p{Upper})|\\s+");
        StringBuilder generatedKey = new StringBuilder();

        for (String word : words) {
            if (word.equals("id")) {
                generatedKey.append("ID");
            } else {
                generatedKey.append(word.charAt(0));
            }
        }

        if (!generatedKey.toString().endsWith("ID")) {
            generatedKey.append("ID");
        }

        while (generatedKey.length() < columnSize) {
            generatedKey.append(Randomizer.generateRandomAlphanumeric(columnSize - generatedKey.length()));
        }

        return generatedKey.toString().toUpperCase();
    }

    // These methods interact with the database to get the last inserted values for INT, LONG, and SERIAL fields

    // Get the last inserted INT value from the database (could be using MAX(id) or LAST_INSERT_ID depending on DB)
    private static int getLastInsertedInt(String tableName, String columnName) throws SQLException {
        String query = "SELECT MAX(" + columnName + ") FROM " + tableName;
        try (Statement stmt = dbConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                throw new SQLException("Failed to retrieve last inserted INT value.");
            }
        }
    }

    // Get the last inserted LONG value from the database
    private static long getLastInsertedLong(String tableName, String columnName) throws SQLException {
        String query = "SELECT MAX(" + columnName + ") FROM " + tableName;
        try (Statement stmt = dbConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                throw new SQLException("Failed to retrieve last inserted LONG value.");
            }
        }
    }

    // Get the next SERIAL value from the database (for PostgreSQL or similar)
    private static int getNextSerialFromDB(String sequenceName) throws SQLException {
        String query = "SELECT nextval('" + sequenceName + "')";
        try (Statement stmt = dbConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                throw new SQLException("Failed to retrieve next SERIAL value.");
            }
        }
    }
}