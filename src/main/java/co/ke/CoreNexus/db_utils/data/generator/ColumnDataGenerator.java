package co.ke.CoreNexus.db_utils.data.generator;

import co.ke.CoreNexus.db_utils.db.metadata.models.ColumnInfo;
import com.github.javafaker.Faker;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.data.generator)
 * Created by: oloo
 * On: 11/11/2024. 23:46
 * Description:
 **/

public class ColumnDataGenerator {

    private final Faker faker;

    public ColumnDataGenerator() {
        this.faker = new Faker();
    }

    public Object generateDataForColumn(ColumnInfo column, Map<String, Object> rowData) {
        String columnName = column.getName().toLowerCase();
        int columnLength = column.getSize();

        // Try context-based generation first
        Object contextData = ContextGenerator.generateFromContext(columnName, columnLength);
        if (contextData != null) {
            return contextData;
        }

        // Fallback to type-based generation using Faker
        return switch (column.getType().toUpperCase()) {
            case "VARCHAR", "TEXT" -> generateStringValue(columnLength);
            case "INTEGER" -> faker.number().randomDigitNotZero();
            case "BIGINT" -> faker.number().randomNumber();
            case "BOOLEAN" -> faker.bool().bool();
            case "DATE" -> faker.date().past(365, TimeUnit.DAYS);
            case "TIMESTAMP" -> faker.date().birthday();
            default -> null;
        };
    }

    private String generateStringValue(int maxLength) {
        String randomValue = faker.lorem().word();
        return randomValue.length() > maxLength ? randomValue.substring(0, maxLength) : randomValue;
    }
}