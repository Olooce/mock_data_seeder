package co.ke.CoreNexus.db_utils.db.metadata;

import co.ke.CoreNexus.db_utils.db.metadata.models.ColumnInfo;
import co.ke.CoreNexus.db_utils.db.metadata.models.SchemaInfo;
import co.ke.CoreNexus.db_utils.db.metadata.models.TableInfo;

import java.sql.*;
import java.util.*;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.db.metadata)
 * Created by: oloo
 * On: 11/11/2024. 20:30
 * Description:
 **/

public class DatabaseSchemaReader {

    private final Connection connection;

    // Constructor to initialize connection
    public DatabaseSchemaReader(Connection connection) {
        this.connection = connection;
    }

    // Method to retrieve schema information
    public Map<String, SchemaInfo> getSchemaInfo() throws SQLException {

        String databaseName = null;
        boolean tablesFound = false;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT DATABASE();")) {
            if (rs.next()) {
                databaseName = rs.getString(1);
                System.out.println("Current database: " + rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Map<String, SchemaInfo> schemas = new HashMap<>();

        // Get database metadata
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet tablesResultSet = metaData.getTables(databaseName, null, null, new String[]{"TABLE"})) {
            while (tablesResultSet.next()) {
                tablesFound = true;
            }
        }

        // Get all schemas
        try (ResultSet schemasResultSet = metaData.getSchemas()) {
            while (schemasResultSet.next()) {
                String schemaName = schemasResultSet.getString("TABLE_SCHEM");
                System.out.println("Found schema: " + schemaName);
                SchemaInfo schemaInfo = new SchemaInfo(schemaName);

                // Get all tables for this schema
                try (ResultSet tablesResultSet = metaData.getTables(null, schemaName, null, new String[]{"TABLE"})) {
                    while (tablesResultSet.next()) {
                        String tableName = tablesResultSet.getString("TABLE_NAME");

                        // Get columns and constraints for each table
                        TableInfo tableInfo = new TableInfo(tableName);
                        retrieveColumns(metaData, schemaName, tableName, tableInfo);
                        retrieveConstraints(metaData, schemaName, tableName, tableInfo);

                        schemaInfo.addTable(tableInfo);
                    }
                }

                schemas.put(schemaName, schemaInfo);
            }

            // If tables are found but no schemas are returned, create the default schema
            if (tablesFound && schemas.isEmpty()) {
                System.out.println("No schemas returned but found tables, creating default schema...");
                SchemaInfo defaultSchema = new SchemaInfo("default");

                // Add all tables to the "default" schema
                try (ResultSet allTablesResultSet = metaData.getTables(databaseName, null, null, new String[]{"TABLE"})) {
                    while (allTablesResultSet.next()) {
                        String tableName = allTablesResultSet.getString("TABLE_NAME");

                        // Get columns and constraints for each table
                        TableInfo tableInfo = new TableInfo(tableName);
                        retrieveColumns(metaData, null, tableName, tableInfo); // Get columns from default schema
                        retrieveConstraints(metaData, null, tableName, tableInfo); // Get constraints from default schema

                        defaultSchema.addTable(tableInfo);
                    }
                }

                schemas.put("default", defaultSchema);
            }
        }

        return schemas;
    }

    // Retrieve columns for a given table
    private void retrieveColumns(DatabaseMetaData metaData, String schemaName, String tableName, TableInfo tableInfo) throws SQLException {
        try (ResultSet columnsResultSet = metaData.getColumns(null, schemaName, tableName, null)) {
            while (columnsResultSet.next()) {
                String columnName = columnsResultSet.getString("COLUMN_NAME");
                String columnType = columnsResultSet.getString("TYPE_NAME");
                int columnSize = columnsResultSet.getInt("COLUMN_SIZE");

                tableInfo.addColumn(new ColumnInfo(columnName, columnType, columnSize));
            }
        }
    }

    // Retrieve constraints (e.g., primary keys, foreign keys)
    private void retrieveConstraints(DatabaseMetaData metaData, String schemaName, String tableName, TableInfo tableInfo) throws SQLException {
        // Get primary keys
        try (ResultSet primaryKeysResultSet = metaData.getPrimaryKeys(null, schemaName, tableName)) {
            while (primaryKeysResultSet.next()) {
                String columnName = primaryKeysResultSet.getString("COLUMN_NAME");
                tableInfo.addPrimaryKey(columnName);
            }
        }

        // Get foreign keys
        try (ResultSet foreignKeysResultSet = metaData.getImportedKeys(null, schemaName, tableName)) {
            while (foreignKeysResultSet.next()) {
                String fkColumn = foreignKeysResultSet.getString("FKCOLUMN_NAME");
                String fkTable = foreignKeysResultSet.getString("PKTABLE_NAME");
                tableInfo.addForeignKey(fkColumn, fkTable);
            }
        }
    }

    // Close the connection when done
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}