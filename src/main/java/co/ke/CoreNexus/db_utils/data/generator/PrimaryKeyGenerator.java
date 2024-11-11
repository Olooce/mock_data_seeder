package co.ke.CoreNexus.db_utils.data.generator;

import co.ke.CoreNexus.db_utils.db.utils.Randomizer;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.data.generator)
 * Created by: oloo
 * On: 11/11/2024. 23:50
 * Description:
 **/
public class PrimaryKeyGenerator {

    private static Connection dbConnection;

    // Set the database connection
    public static void setDbConnection(Connection connection) {
        dbConnection = connection;
    }

    // Generate the primary key based on column type and auto-increment status
    public static Object generatePrimaryKeyFromColumnName(String columnName, String type, int columnSize, boolean isAutoIncrement) throws SQLException {
        if (isAutoIncrement || isAutoIncrementType(type)) {
            // Auto-increment or SERIAL fields - let the database handle this
            return "";  // Empty string means the DB will handle it
        }

        // Handle INT and LONG types, generating the next value in sequence
        if (type.equalsIgnoreCase("int")) {
            return generateNextInt();
        } else if (type.equalsIgnoreCase("long")) {
            return generateNextLong();
        } else if (type.equalsIgnoreCase("serial")) {
            return generateNextSerial();
        }

        // If it's not numeric or serial, handle it as a string-based primary key
        return generateStringPrimaryKey(columnName, columnSize);
    }

    // Check if the column is auto-increment or SERIAL
    private static boolean isAutoIncrementType(String type) {
        return type.equalsIgnoreCase("auto_increment") || type.equalsIgnoreCase("serial");
    }

    // Generate the next INT value in sequence (assuming INT is the primary key)
    private static int generateNextInt() throws SQLException {
        return getLastInsertedInt() + 1;
    }

    // Generate the next LONG value in sequence (assuming LONG is the primary key)
    private static long generateNextLong() throws SQLException {
        return getLastInsertedLong() + 1L;
    }

    // Generate the next SERIAL value (for databases that support SERIAL, like PostgreSQL)
    private static int generateNextSerial() throws SQLException {
        return getNextSerialFromDB();
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
    private static int getLastInsertedInt() throws SQLException {
        String query = "SELECT MAX(id) FROM your_table"; // Adjust the table name
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
    private static long getLastInsertedLong() throws SQLException {
        String query = "SELECT MAX(id) FROM your_table"; // Adjust the table name
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
    private static int getNextSerialFromDB() throws SQLException {
        String query = "SELECT nextval('your_sequence_name')"; // Adjust the sequence name
        try (Statement stmt = dbConnection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                throw new SQLException("Failed to retrieve next SERIAL value.");
            }
        }
    }
}}