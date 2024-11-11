package co.ke.CoreNexus.db_utils;

import co.ke.CoreNexus.db_utils.data.generator.DataGenerator;
import co.ke.CoreNexus.db_utils.db.connection.DatabaseConnector;
import co.ke.CoreNexus.db_utils.db.metadata.DatabaseSchemaReader;
import co.ke.CoreNexus.db_utils.db.metadata.models.SchemaInfo;
import co.ke.CoreNexus.db_utils.db.metadata.models.TableInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * mock_data_seedar (co.ke.CoreNexus.db_utils)
 * Created by: oloo
 * On: 11/11/2024. 19:27
 * Description:
 **/

public class Seeder {

    public static void main(String[] args) {
        // Number of rows to insert per table
        int rowCount = 50;

        try (Connection connection = DatabaseConnector.getConnection()) {
            // Retrieve metadata for all schemas in the database
            DatabaseSchemaReader databaseMetadata = new DatabaseSchemaReader(connection);
            Map<String, SchemaInfo> schemas = databaseMetadata.getSchemaInfo();

            System.out.println("Schemas found: " + schemas.size());
            if (schemas.isEmpty()) {
                System.out.println("No schemas found in the database.");
            }

            // Initialize data generator
            DataGenerator dataGenerator = new DataGenerator();

            // Seed data for each schema and its tables
            for (SchemaInfo schema : schemas.values()) {
                System.out.println("Processing schema: " + schema.getName());
                for (TableInfo table : schema.getTables().values()) {
                    System.out.println("Seeding data for table: " + table.getName());
                    List<Map<String, Object>> generatedData = dataGenerator.generateDataForTable(table, rowCount);
                    System.out.println("Generated data for table " + table.getName() + ": " + generatedData.size() + " rows");
                    if (!generatedData.isEmpty()) {
                        insertDataIntoTable(connection, table, generatedData);
                    } else {
                        System.out.println("No data generated for table: " + table.getName());
                    }
                }
            }

            System.out.println("Seeding completed.");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DatabaseConnector.close();
        }
    }

    private static void insertDataIntoTable(Connection connection, TableInfo table, List<Map<String, Object>> data) {
        String tableName = table.getName();
        List<String> columnNames = table.getColumnNames();

        // Build SQL insert statement dynamically based on column names
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " (");
        sql.append(String.join(", ", columnNames));
        sql.append(") VALUES (");
        sql.append("?,".repeat(columnNames.size()).substring(0, columnNames.size() * 2 - 1));
        sql.append(")");

        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            for (Map<String, Object> row : data) {
                for (int i = 0; i < columnNames.size(); i++) {
                    stmt.setObject(i + 1, row.get(columnNames.get(i)));
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
            System.out.println("Data inserted into table: " + tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
