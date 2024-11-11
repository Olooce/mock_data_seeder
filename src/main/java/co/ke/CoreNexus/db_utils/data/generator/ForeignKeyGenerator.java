package co.ke.CoreNexus.db_utils.data.generator;

import co.ke.CoreNexus.db_utils.db.connection.DatabaseConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.data.generator)
 * Created by: oloo
 * On: 11/11/2024. 23:52
 * Description:
 **/
public class ForeignKeyGenerator {

    public Object generateForeignKeyValue(String referencedTable, String referencedColumn) {
        List<Object> validForeignKeys = getForeignKeyValuesFromReferencedTable(referencedTable, referencedColumn);

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
            e.printStackTrace();
        }

        return foreignKeyValues;
    }
}
