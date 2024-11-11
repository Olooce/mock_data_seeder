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

    private final PrimaryKeyGenerator primaryKeyGenerator;
    private final ForeignKeyGenerator foreignKeyGenerator;
    private final ColumnDataGenerator columnDataGenerator;

    public DataGenerator() {
        this.primaryKeyGenerator = new PrimaryKeyGenerator();
        this.foreignKeyGenerator = new ForeignKeyGenerator();
        this.columnDataGenerator = new ColumnDataGenerator();
    }

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

    private Object generateDataForColumn(ColumnInfo column, Map<String, Object> rowData, TableInfo tableInfo) {
        String columnName = column.getName().toLowerCase();

        if (tableInfo.getPrimaryKeys().contains(columnName)) {
            return primaryKeyGenerator.generatePrimaryKeyFromColumnName(columnName, column.getSize());
        }

        if (tableInfo.getForeignKeys().containsKey(columnName)) {
            return foreignKeyGenerator.generateForeignKeyValue(tableInfo.getForeignKeys().get(columnName), columnName);
        }

        return columnDataGenerator.generateDataForColumn(column, rowData);
    }
}