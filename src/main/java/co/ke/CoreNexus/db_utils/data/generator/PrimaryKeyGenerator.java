package co.ke.CoreNexus.db_utils.data.generator;

import co.ke.CoreNexus.db_utils.db.utils.Randomizer;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.data.generator)
 * Created by: oloo
 * On: 11/11/2024. 23:50
 * Description:
 **/

public class PrimaryKeyGenerator {

    public String generatePrimaryKeyFromColumnName(String columnName, int columnSize) {
        String normalizedColumnName = columnName.toLowerCase().replaceAll("_", " ");
        String[] words = normalizedColumnName.split("(?<=.)(?=\\p{Upper})|\\s+");
        StringBuilder generatedKey = new StringBuilder();

        for (String word : words) {
            if (word.equals("id")) {
                generatedKey.append("ID");
            } else {
                generatedKey.append(word.charAt(0));
            }
        }

        if (!generatedKey.toString().endsWith("ID")) {
            generatedKey.append("ID");
        }

        while (generatedKey.length() < columnSize) {
            generatedKey.append(Randomizer.generateRandomAlphanumeric(columnSize - generatedKey.length()));
        }

        return generatedKey.toString().toUpperCase();
    }
}