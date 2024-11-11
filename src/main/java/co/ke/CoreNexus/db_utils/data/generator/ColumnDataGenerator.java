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

        // Context-based generation
        if (isNameColumn(columnName)) {
            return faker.name().fullName();
        }

        if (isEmailColumn(columnName)) {
            return faker.internet().emailAddress();
        }

        if (isPhoneColumn(columnName)) {
            return faker.phoneNumber().cellPhone();
        }

        if (isAddressColumn(columnName)) {
            return faker.address().fullAddress();
        }

        if (isDateColumn(columnName)) {
            return faker.date().birthday();
        }

        if (isGenderColumn(columnName)) {
            return faker.bool().bool() ? "Male" : "Female";
        }

        if (isAgeColumn(columnName)) {
            return faker.number().numberBetween(18, 100);
        }

        // Handle other types using Faker based on column type
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

    private boolean isNameColumn(String columnName) {
        return columnName.contains("name") || columnName.contains("full_name") || columnName.contains("first_name");
    }

    private boolean isEmailColumn(String columnName) {
        return columnName.contains("email");
    }

    private boolean isPhoneColumn(String columnName) {
        return columnName.contains("phone") || columnName.contains("mobile");
    }

    private boolean isAddressColumn(String columnName) {
        return columnName.contains("address") || columnName.contains("street");
    }

    private boolean isDateColumn(String columnName) {
        return columnName.contains("dob") || columnName.contains("birth") || columnName.contains("date");
    }

    private boolean isGenderColumn(String columnName) {
        return columnName.contains("gender");
    }

    private boolean isAgeColumn(String columnName) {
        return columnName.contains("age");
    }

    private String generateStringValue(int maxLength) {
        String randomValue = faker.lorem().word();
        return randomValue.length() > maxLength ? randomValue.substring(0, maxLength) : randomValue;
    }
}
