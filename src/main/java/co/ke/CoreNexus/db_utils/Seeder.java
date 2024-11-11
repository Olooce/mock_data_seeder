package co.ke.CoreNexus.db_utils;

import co.ke.CoreNexus.db_utils.data.generator.DataGenerator;
import co.ke.CoreNexus.db_utils.db.connection.DatabaseConnector;
import co.ke.CoreNexus.db_utils.db.metadata.DatabaseSchemaReader;
import co.ke.CoreNexus.db_utils.db.metadata.models.SchemaInfo;
import co.ke.CoreNexus.db_utils.db.metadata.models.TableInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mock_data_seedar (co.ke.CoreNexus.db_utils)
 * Created by: oloo
 * On: 11/11/2024. 19:27
 * Description:
 **/
public class Seeder {
    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    private static final AtomicInteger insertedRecords = new AtomicInteger(0);
    private static final ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public static void main(String[] args) {
        Thread statusLogger = new Thread(new StatusLogger(), "StatusLogger");
        statusLogger.start();

        int rowCount = 500000;

        try (Connection connection = DatabaseConnector.getConnection()) {
            DatabaseSchemaReader databaseMetadata = new DatabaseSchemaReader(connection);
            Map<String, SchemaInfo> schemas = databaseMetadata.getSchemaInfo();

            System.out.println("Schemas found: " + schemas.size());
            if (schemas.isEmpty()) {
                System.out.println("No schemas found in the database.");
                return;
            }

            DataGenerator dataGenerator = new DataGenerator();

            // Process each schema in parallel
            List<Callable<Void>> tasks = new ArrayList<>();
            for (SchemaInfo schema : schemas.values()) {
                tasks.add(() -> {
                    processSchema(dataGenerator, schema, rowCount);
                    return null;
                });
            }

            // Execute all tasks in parallel
            executor.invokeAll(tasks);

            System.out.println("Seeding completed.");
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
            DatabaseConnector.close();
            statusLogger.interrupt();  // Stop the status logger
        }
    }

    private static void processSchema(DataGenerator dataGenerator, SchemaInfo schema, int rowCount) {
        System.out.println("Processing schema: " + schema.getName());

        // Create tasks for each table
        List<Callable<Void>> tableTasks = new ArrayList<>();
        for (TableInfo table : getTablesInInsertOrder(new ArrayList<>(schema.getTables().values()), schema.getTables())) {
            tableTasks.add(() -> {
                try (Connection connection = DatabaseConnector.getConnection()) { // New connection for each task
                    List<Map<String, Object>> generatedData = dataGenerator.generateDataForTable(table, rowCount);
                    if (!generatedData.isEmpty()) {
                        insertDataIntoTable(connection, table, generatedData);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            });
        }

        try {
            executor.invokeAll(tableTasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static List<TableInfo> getTablesInInsertOrder(List<TableInfo> tables, Map<String, TableInfo> tableMap) {
        List<TableInfo> orderedTables = new ArrayList<>();
        Set<String> processedTables = new HashSet<>();

        // First pass: Identify tables with no foreign keys
        for (TableInfo table : tables) {
            if (hasNoForeignKeyConstraints(table)) {
                orderedTables.add(table);
                processedTables.add(table.getName());
            }
        }

        // Second pass: Process tables with foreign key dependencies
        boolean added = true;
        while (processedTables.size() < tables.size() && added) {
            added = false;
            for (TableInfo table : tables) {
                if (!processedTables.contains(table.getName()) && canBeInserted(table, processedTables, tableMap)) {
                    orderedTables.add(table);
                    processedTables.add(table.getName());
                    added = true;  // Mark that we've added a table
                }
            }
        }

        return orderedTables;
    }

    private static boolean hasNoForeignKeyConstraints(TableInfo table) {
        return table.getForeignKeys() == null || table.getForeignKeys().isEmpty();
    }

    private static boolean canBeInserted(TableInfo table, Set<String> processedTables, Map<String, TableInfo> tableMap) {
        Map<String, String> foreignKeys = table.getForeignKeys();
        for (Map.Entry<String, String> entry : foreignKeys.entrySet()) {
            String referencedTable = entry.getValue();  // Get the referenced table
            if (!processedTables.contains(referencedTable)) {
                return false;  // Can't insert this table until the referenced table is processed
            }
        }
        return true;
    }


    private static void insertDataIntoTable(Connection connection, TableInfo table, List<Map<String, Object>> data) throws SQLException {
        String tableName = table.getName();
        List<String> columnNames = table.getColumnNames();

        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (")
                .append(String.join(", ", columnNames)).append(") VALUES (")
                .append("?,".repeat(columnNames.size()).substring(0, columnNames.size() * 2 - 1)).append(")");

        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            connection.setAutoCommit(false);

            for (Map<String, Object> row : data) {
                for (int i = 0; i < columnNames.size(); i++) {
                    stmt.setObject(i + 1, row.get(columnNames.get(i)));
                }
                stmt.addBatch();
                insertedRecords.incrementAndGet();
            }

            stmt.executeBatch();
            connection.commit();
            System.out.println("Data inserted into table: " + tableName);
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);  // Reset to default
        }
    }

    // Nested class to handle status logging
    private static class StatusLogger implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                System.out.println("Total records inserted so far: " + insertedRecords.get() + " at " + new Date());
                try {
                    Thread.sleep(5000);  // Log status every 5 seconds
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

