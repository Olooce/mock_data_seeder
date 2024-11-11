package co.ke.CoreNexus.db_utils.data.generator;

import co.ke.CoreNexus.db_utils.db.connection.DatabaseConnector;
import co.ke.CoreNexus.db_utils.db.metadata.models.ColumnInfo;
import co.ke.CoreNexus.db_utils.db.metadata.models.TableInfo;
import com.github.javafaker.Faker;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.*;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.data.generator)
 * Created by: oloo
 * On: 11/11/2024. 20:43
 * Description:
 **/

public class DataGenerator {

    private Faker faker;
    private DatabaseConnector databaseConnector;

    // Constructor initializes Faker instance
    public DataGenerator(DatabaseConnector databaseConnector) {
        this.faker = new Faker();
        this.databaseConnector = databaseConnector;
    }

    // Method to generate random data for a table based on its schema
    public List<Map<String, Object>> generateDataForTable(TableInfo tableInfo, int rowCount) {
        List<Map<String, Object>> generatedData = new ArrayList<>();

        for (int i = 0; i < rowCount; i++) {
            Map<String, Object> rowData = new HashMap<>();
            for (ColumnInfo column : tableInfo.getColumns()) {
                Object generatedValue = generateDataForColumn(column, rowData);
                rowData.put(column.getName(), generatedValue);
            }
            generatedData.add(rowData);
        }
        return generatedData;
    }

    // Method to generate data for a specific column based on its type and name
    private Object generateDataForColumn(ColumnInfo column, Map<String, Object> rowData) {
        String columnName = column.getName().toLowerCase();

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

        // For other standard columns, use data types
        return switch (column.getType().toUpperCase()) {
            case "VARCHAR", "TEXT" -> faker.lorem().word(); // Generates a random word
            case "INTEGER" -> faker.number().randomDigitNotZero(); // Generates a random non-zero integer
            case "BIGINT" -> faker.number().randomNumber(); // Generates a random large number
            case "BOOLEAN" -> faker.bool().bool(); // Generates a random boolean value
            case "DATE" -> faker.date().past(365, TimeUnit.DAYS); // Generates a random past date
            case "TIMESTAMP" -> faker.date().birthday(); // Generates a random timestamp (birthday for simplicity)
            default -> null; // If unknown type, return null
        };
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
    public Set<Object> generateUniqueValues(ColumnInfo column, int count) {
        Set<Object> uniqueValues = new HashSet<>();
        while (uniqueValues.size() < count) {
            uniqueValues.add(generateDataForColumn(column, new HashMap<>()));
        }
        return uniqueValues;
    }

    public Object generateForeignKeyValue(String referencedTable, String referencedColumn, String columnType) {
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

        try (Connection conn = databaseConnector.getConnection();
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

    public List<Object> generateDataForTable(List<ColumnInfo> columns) {
        List<Object> rowData = new ArrayList<>();

        for (ColumnInfo column : columns) {
            if (column.isForeignKey()) {
                // Generate foreign key value based on the column reference
                Object fkValue = generateForeignKeyValue(column.getReferencedTable(), column.getReferencedColumn(), column.getType());
                rowData.add(fkValue);
            } else {
                // Generate data for regular column
                Object value = generateDataForColumn(column);
                rowData.add(value);
            }
        }

        return rowData;
    }

}