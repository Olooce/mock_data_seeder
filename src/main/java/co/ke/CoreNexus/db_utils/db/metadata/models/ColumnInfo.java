package co.ke.CoreNexus.db_utils.db.metadata.models;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.db.metadata.models)
 * Created by: oloo
 * On: 11/11/2024. 20:33
 * Description:
 **/

public class ColumnInfo {

    private String name;
    private String type;
    private int size;

    public ColumnInfo(String name, String type, int size) {
        this.name = name;
        this.type = type;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public int getSize() {
        return size;
    }
}