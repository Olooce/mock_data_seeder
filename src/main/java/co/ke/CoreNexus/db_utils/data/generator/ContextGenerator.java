package co.ke.CoreNexus.db_utils.data.generator;

import com.github.javafaker.Faker;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.data.generator)
 * Created by: oloo
 * On: 12/11/2024. 00:11
 * Description:
 **/
public class ContextGenerator {

    private static final Faker faker = new Faker();

    public static Object generateFromContext(String columnName, int columnLength) {
        // Handle context-based generation here based on column names or patterns
        if (columnName.contains("email")) {
            return generateEmail(columnLength);
        } else if (columnName.contains("phone")) {
            return generatePhoneNumber(columnLength);
        } else if (columnName.contains("address")) {
            return generateAddress(columnLength);
        } else if (columnName.contains("name")) {
            return generateName(columnLength);
        } else if (columnName.contains("gender")) {
            return generateGender();
        } else if (columnName.contains("date")) {
            return generateDate();
        }
        return null;  // No context-based generation required
    }

    private static String generateEmail(int columnLength) {
        String email = faker.internet().emailAddress();
        return email.length() > columnLength ? email.substring(0, columnLength) : email;
    }

    private static String generatePhoneNumber(int columnLength) {
        String phone = faker.phoneNumber().phoneNumber();
        return phone.length() > columnLength ? phone.substring(0, columnLength) : phone;
    }

    private static String generateAddress(int columnLength) {
        String address = faker.address().fullAddress();
        return address.length() > columnLength ? address.substring(0, columnLength) : address;
    }

    private static String generateName(int columnLength) {
        String name = faker.name().fullName();
        return name.length() > columnLength ? name.substring(0, columnLength) : name;
    }

    private static String generateGender() {
        return faker.gender().binaryType();
    }

    private static String generateDate() {
        return faker.date().birthday().toString();  // Generate random date as string
    }
}