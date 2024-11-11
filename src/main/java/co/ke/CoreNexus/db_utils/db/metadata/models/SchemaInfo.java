package co.ke.CoreNexus.db_utils.db.metadata.models;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.db.metadata.models)
 * Created by: oloo
 * On: 11/11/2024. 20:32
 * Description:
 **/

public class SchemaInfo {

    private String name;
    private Map<String, TableInfo> tables;

    public SchemaInfo(String name) {
        this.name = name;
        this.tables = new HashMap<>();
    }

    public void addTable(TableInfo tableInfo) {
        tables.put(tableInfo.getName(), tableInfo);
    }

    public String getName() {
        return name;
    }

    public Map<String, TableInfo> getTables() {
        return tables;
    }
}