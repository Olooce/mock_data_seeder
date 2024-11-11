package co.ke.CoreNexus.db_utils.db.metadata.models;

import java.util.*;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.db.metadata.models)
 * Created by: oloo
 * On: 11/11/2024. 20:32
 * Description:
 **/

public class TableInfo {

    private String name;
    private List<ColumnInfo> columns;
    private Set<String> primaryKeys;
    private Map<String, String> foreignKeys; // Column -> Table

    public TableInfo(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
        this.primaryKeys = new HashSet<>();
        this.foreignKeys = new HashMap<>();
    }

    public void addColumn(ColumnInfo columnInfo) {
        columns.add(columnInfo);
    }

    public void addPrimaryKey(String columnName) {
        primaryKeys.add(columnName);
    }

    public void addForeignKey(String columnName, String tableName) {
        foreignKeys.put(columnName, tableName);
    }

    public String getName() {
        return name;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public Set<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public Map<String, String> getForeignKeys() {
        return foreignKeys;
    }
}