package co.ke.CoreNexus.db_utils.data.generator;

import co.ke.CoreNexus.db_utils.db.connection.DatabaseConnector;
import co.ke.CoreNexus.db_utils.db.metadata.models.ColumnInfo;
import co.ke.CoreNexus.db_utils.db.metadata.models.TableInfo;
import co.ke.CoreNexus.db_utils.db.utils.Randomizer;
import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.data.generator)
 * Created by: oloo
 * On: 11/11/2024. 20:43
 * Description:
 **/
public class DataGenerator {

    private final Faker faker;

    // Constructor initializes Faker instance
    public DataGenerator() {
        this.faker = new Faker();
    }

    // Method to generate random data for a table based on its schema
    public List<Map<String, Object>> generateDataForTable(TableInfo tableInfo, int rowCount) {
        List<Map<String, Object>> generatedData = new ArrayList<>();

        for (int i = 0; i < rowCount; i++) {
            Map<String, Object> rowData = new HashMap<>();
            for (ColumnInfo column : tableInfo.getColumns()) {
                Object generatedValue = generateDataForColumn(column, rowData, tableInfo);
                rowData.put(column.getName(), generatedValue);
            }
            generatedData.add(rowData);
        }
        return generatedData;
    }

    // Method to generate data for a specific column based on its type and name
    private Object generateDataForColumn(ColumnInfo column, Map<String, Object> rowData, TableInfo tableInfo) {
        String columnName = column.getName().toLowerCase();
        int columnLength = column.getSize();

        // Check if the column is part of the primary key
        if (tableInfo.getPrimaryKeys().contains(columnName)) {
            // For primary keys, generate based on column name pattern
            String primaryKeyValue = generatePrimaryKeyFromColumnName(columnName);

            // Append random alphanumeric characters to meet the column length
            StringBuilder generatedKey = new StringBuilder(primaryKeyValue);
            while (generatedKey.length() < columnLength) {
                generatedKey.append(Randomizer.generateRandomAlphanumeric(1));  // Append a random alphanumeric character
            }

            return generatedKey.toString();
        }

        // Check if the column is a foreign key
        if (tableInfo.getForeignKeys().containsKey(columnName)) {
            // Get the referenced table for the foreign key
            String referencedTable = tableInfo.getForeignKeys().get(columnName);

            // Generate a valid foreign key value (this could involve querying the referenced table for a valid key)
            return generateForeignKeyValue(referencedTable, columnName);
        }

        // Gender-based logic based on column name or first name
        if (columnName.contains("gender")) {
            return generateGenderBasedOnName(rowData);
        }

        // Other custom logic based on column names
        if (columnName.contains("name")) {
            return faker.name().firstName();  // Generate first name
        }

        if (columnName.contains("address")) {
            return faker.address().streetAddress();  // Generate a random address
        }

        if (columnName.contains("dob") || columnName.contains("birth")) {
            return faker.date().birthday();  // Generate a random birthday
        }

        if (columnName.contains("age")) {
            return faker.number().numberBetween(18, 100);  // Generate a random age
        }

        if (columnName.contains("email")) {
            return faker.internet().emailAddress();  // Generate an email address
        }

        // For other standard columns, use data types and respect column length
        return switch (column.getType().toUpperCase()) {
            case "VARCHAR", "TEXT" -> generateStringValue(columnLength); // Respect string length
            case "INTEGER" -> faker.number().randomDigitNotZero(); // Generates a random non-zero integer
            case "BIGINT" -> faker.number().randomNumber(); // Generates a random large number
            case "BOOLEAN" -> faker.bool().bool(); // Generates a random boolean value
            case "DATE" -> faker.date().past(365, TimeUnit.DAYS); // Generates a random past date
            case "TIMESTAMP" -> faker.date().birthday(); // Generates a random timestamp (birthday for simplicity)
            default -> null; // If unknown type, return null
        };
    }

    // Method to generate primary key based on column name
    private String generatePrimaryKeyFromColumnName(String columnName) {
        // Normalize the column name to lowercase, remove underscores, and split by camelCase or snake_case
        String normalizedColumnName = columnName.toLowerCase().replaceAll("_", " ");

        // Split based on camel case (e.g., "staffId" -> "staff", "Id")
        String[] words = normalizedColumnName.split("(?<=.)(?=\\p{Upper})|\\s+");

        StringBuilder generatedKey = new StringBuilder();

        // Loop through the words and append the first letter of each word
        for (String word : words) {
            if (word.equals("id")) {
                generatedKey.append("ID");  // Special handling for 'id' to append 'ID'
            } else {
                generatedKey.append(word.charAt(0));  // Append the first letter of the word
            }
        }

        // Check if 'ID' is at the end of the key, if not, append it
        if (!generatedKey.toString().endsWith("ID")) {
            generatedKey.append("ID");
        }

        return generatedKey.toString().toUpperCase();
    }


    // Method to generate a random string that respects the column length
    private String generateStringValue(int maxLength) {
        String randomValue = faker.lorem().word();
        return randomValue.length() > maxLength ? randomValue.substring(0, maxLength) : randomValue;
    }

    // Method to generate gender based on the first name provided
    private String generateGenderBasedOnName(Map<String, Object> rowData) {
        // If there is a name field, try to derive the gender
        if (rowData.containsKey("first_name")) {
            String firstName = (String) rowData.get("first_name");
            // Simple gender determination based on name, can be expanded
            if (firstName.equalsIgnoreCase("John") || firstName.equalsIgnoreCase("Mike") || firstName.equalsIgnoreCase("David")) {
                return "Male";
            } else if (firstName.equalsIgnoreCase("Jane") || firstName.equalsIgnoreCase("Mary") || firstName.equalsIgnoreCase("Emma")) {
                return "Female";
            }
        }
        // Default if no name is present, could use random gender generation
        return faker.bool().bool() ? "Male" : "Female";
    }

    // Method to generate unique values (to respect UNIQUE constraints)
    public Set<Object> generateUniqueValues(TableInfo tableInfo, ColumnInfo column, int count) {
        Set<Object> uniqueValues = new HashSet<>();
        while (uniqueValues.size() < count) {
            uniqueValues.add(generateDataForColumn(column, new HashMap<>(), tableInfo));
        }
        return uniqueValues;
    }

    public Object generateForeignKeyValue(String referencedTable, String referencedColumn) {
        List<Object> validForeignKeys = getForeignKeyValuesFromReferencedTable(referencedTable, referencedColumn);

        // Randomly select a foreign key value from the valid values retrieved
        if (!validForeignKeys.isEmpty()) {
            return validForeignKeys.get((int) (Math.random() * validForeignKeys.size()));
        }

        return null;  // No valid foreign key values available
    }

    private List<Object> getForeignKeyValuesFromReferencedTable(String referencedTable, String referencedColumn) {
        List<Object> foreignKeyValues = new ArrayList<>();
        String query = String.format("SELECT %s FROM %s", referencedColumn, referencedTable);

        try (Connection conn = DatabaseConnector.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                Object foreignKeyValue = rs.getObject(referencedColumn);
                foreignKeyValues.add(foreignKeyValue);
            }
        } catch (SQLException e) {
            e.printStackTrace();  // Handle exception properly
        }

        return foreignKeyValues;
    }

}