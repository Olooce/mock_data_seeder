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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mock_data_seedar (co.ke.CoreNexus.db_utils)
 * Created by: oloo
 * On: 11/11/2024. 19:27
 * Description:
 **/

public class Seeder {
    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;
    private static final int CHUNK_SIZE = 500;
    private static final int BATCH_SIZE = 100;

    //TODO: load variables as configs from external xml file. Hardcoding values doesn't make sense

    private static final AtomicInteger insertedRecords = new AtomicInteger(0);
    private static final ThreadPoolExecutor schemaExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(THREAD_POOL_SIZE / 2);
    private static final ThreadPoolExecutor chunkExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(THREAD_POOL_SIZE / 2);


    public static void main(String[] args) {
        Thread statusLogger = new Thread(new StatusLogger(), "StatusLogger");
        int rowCount = 5000;

        try (Connection connection = DatabaseConnector.getConnection()) {
            DatabaseSchemaReader databaseMetadata = new DatabaseSchemaReader(connection);
            Map<String, SchemaInfo> schemas = databaseMetadata.getSchemaInfo();

            if (schemas.isEmpty()) {
                System.out.println("No schemas found in the database.");
                return;
            }

            List<Callable<Void>> schemaTasks = new ArrayList<>();

            for (SchemaInfo schema : schemas.values()) {
                schemaTasks.add(() -> {
                    processSchema(schema, rowCount);
                    return null;
                });
            }

            statusLogger.start();
            schemaExecutor.invokeAll(schemaTasks); // Run all schemas in parallel

            // Wait for all schema executor tasks to complete
            schemaExecutor.shutdown();
            schemaExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            // After schema executor finishes, shutdown chunk executor
            chunkExecutor.shutdown();
            chunkExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            System.out.println("Seeding completed.");
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Total records inserted: " + insertedRecords.get() + ". Stopped at " + new Date());
            DatabaseConnector.close();
            statusLogger.interrupt();  // Stop the status logger
        }
    }

    private static void processSchema(SchemaInfo schema, int rowCount) {
        for (TableInfo table : getTablesInInsertOrder(new ArrayList<>(schema.getTables().values()), schema.getTables())) {
            schemaExecutor.submit(() -> {
                try  {
                    generateAndInsertDataInChunks(table, rowCount);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static void generateAndInsertDataInChunks(TableInfo table, int rowCount) throws SQLException {
        int generatedCount = 0;

        DataGenerator dataGenerator = new DataGenerator();

        while (generatedCount < rowCount) {
            int remaining = rowCount - generatedCount;
            int currentChunkSize = Math.min(Seeder.CHUNK_SIZE, remaining);

            List<Map<String, Object>> generatedData = dataGenerator.generateDataForTable(table, currentChunkSize);
            if (!generatedData.isEmpty()) {
                chunkExecutor.submit(() -> {
                    try (Connection connection = DatabaseConnector.getConnection()){
                        insertDataIntoTable(connection, table, generatedData);
                        insertedRecords.addAndGet(generatedData.size());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
                generatedCount += generatedData.size();
                randomSleep();
            }
        }
    }

    private static List<TableInfo> getTablesInInsertOrder(List<TableInfo> tables, Map<String, TableInfo> tableMap) {
        List<TableInfo> orderedTables = new ArrayList<>();
        Set<String> processedTables = new HashSet<>();

        for (TableInfo table : tables) {
            if (hasNoForeignKeyConstraints(table)) {
                orderedTables.add(table);
                processedTables.add(table.getName());
            }
        }

        boolean added = true;
        while (processedTables.size() < tables.size() && added) {
            added = false;
            for (TableInfo table : tables) {
                if (!processedTables.contains(table.getName()) && canBeInserted(table, processedTables, tableMap)) {
                    orderedTables.add(table);
                    processedTables.add(table.getName());
                    added = true;
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
            String referencedTable = entry.getValue();
            if (!processedTables.contains(referencedTable)) {
                return false;
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

                // Execute the batch if it reaches the batch size
                int count = 0;
                if (++count % Seeder.BATCH_SIZE == 0) {
                    stmt.executeBatch();
                    connection.commit();
                }
            }

            // Execute remaining rows
            stmt.executeBatch();
            connection.commit();
        }
    }

    private static void randomSleep() {
        try {
            int sleepTime = ThreadLocalRandom.current().nextInt(100, 501);
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static class StatusLogger implements Runnable {
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                System.out.println("Total records inserted so far: " + insertedRecords.get() + " at " + new Date());
                System.out.println("Active threads: " + schemaExecutor.getActiveCount() + chunkExecutor.getActiveCount());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}