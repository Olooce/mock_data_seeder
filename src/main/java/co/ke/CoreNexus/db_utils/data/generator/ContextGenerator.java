package co.ke.CoreNexus.db_utils.data.generator;

import com.github.javafaker.Faker;

import java.util.Date;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.data.generator)
 * Created by: oloo
 * On: 12/11/2024. 00:11
 * Description:
 **/

public class ContextGenerator {

    private static final Faker faker = new Faker();

    public static Object generateFromContext(String columnName, String columnType, int columnLength) {
        columnName = columnName.toLowerCase();

        // Context-based rules for generating data based on column name
        if (columnName.contains("author")) {
            return generateAuthorName(columnLength); // realistic author name
        } else if (columnName.contains("publisher")) {
            return generatePublisherName(); // realistic publisher name
        } else if (columnName.contains("language")) {
            return generateLanguage(); // realistic language name
        } else if (columnName.contains("isbn")) {
            return generateIsbn(); // realistic ISBN
        } else if (columnName.contains("email")) {
            return generateEmail(); // realistic email
        } else if (columnName.contains("phone")) {
            return generatePhoneNumber(); // realistic phone number
        } else if (columnName.contains("price") || columnName.contains("amount") || columnName.contains("fee")) {
            return generatePrice(); // realistic price or fee
        } else if (columnName.contains("salary") || columnName.contains("income")) {
            return generateSalary(); // realistic salary
        } else if (columnName.contains("transaction")) {
            return generateTransactionId(); // realistic transaction ID
        } else if (columnName.contains("publication_year") || columnName.contains("year")) {
            return generateYear(); // realistic year of publication
        } else if (columnName.contains("address")) {
            return generateAddress(); // realistic address
        } else if (columnName.contains("department") || columnName.contains("course")) {
            return generateDepartmentOrCourse(); // realistic department or course name
        } else if (columnName.contains("subject")) {
            return generateSubject(); // realistic subject or field of study
        } else if (columnName.contains("company")) {
            return generateCompanyName(); // realistic company name
        } else if (columnName.contains("research") || columnName.contains("study")) {
            return generateResearchStudy(); // realistic research study or paper
        } else if (columnName.contains("degree")) {
            return generateDegree(); // realistic degree
        } else if (columnName.contains("project")) {
            return generateProjectTitle(); // realistic project title
        } else if (columnName.contains("url")) {
            return generateUrl(); // realistic URL
        }

        // Add more context-based handling as needed

        return null; // Return null if no context-based generation is applied
    }

    private static String generateAuthorName(int maxLength) {
        String authorName = faker.book().author();
        return authorName.length() > maxLength ? authorName.substring(0, maxLength) : authorName;
    }

    private static String generateLanguage() {
        // Return a random language from a predefined list
        String[] languages = {"English", "Spanish", "French", "German", "Chinese", "Japanese", "Russian", "Italian", "Portuguese"};
        return languages[faker.number().numberBetween(0, languages.length)];
    }

    private static String generatePublisherName() {
        // Simulate publisher names using Faker's company name
        return faker.company().name();
    }

    private static String generateEmail() {
        // Generate a random email address
        return faker.internet().emailAddress();
    }

    private static String generatePhoneNumber() {
        // Generate a phone number in the format (XXX) XXX-XXXX
        return faker.phoneNumber().cellPhone();
    }

    private static String generateIsbn() {
        // Generate a realistic ISBN number (simplified format)
        return faker.number().digits(13);  // ISBN typically has 13 digits
    }

    private static String generatePrice() {
        // Generate a realistic price or amount (decimal value)
        return "$" + faker.commerce().price();
    }

    private static String generateSalary() {
        // Generate a realistic salary
        return "$" + faker.number().numberBetween(40000, 200000); // Random salary range
    }

    private static String generateTransactionId() {
        // Generate a realistic transaction ID (UUID style)
        return faker.idNumber().valid();
    }

    private static String generateYear() {
        // Generate a random year between 1900 and the current year
        return String.valueOf(faker.number().numberBetween(1900, 2025));
    }

    private static String generateAddress() {
        // Generate a random address
        return faker.address().fullAddress();
    }

    private static String generateDepartmentOrCourse() {
        // Generate a random department or course name
        String[] departments = {"Computer Science", "Engineering", "Physics", "Mathematics", "Literature", "Biology", "Finance"};
        return departments[faker.number().numberBetween(0, departments.length)];
    }

    private static String generateSubject() {
        // Generate a random subject or field of study
        String[] subjects = {"Artificial Intelligence", "Quantum Physics", "Software Engineering", "Economics", "Data Science", "History"};
        return subjects[faker.number().numberBetween(0, subjects.length)];
    }

    private static String generateCompanyName() {
        // Generate a realistic company name
        return faker.company().name();
    }

    private static String generateResearchStudy() {
        // Generate a random research study title
        String[] studies = {"Deep Learning in Computer Vision", "Quantum Computing Advances", "Financial Modeling and Predictions", "Sustainable Energy Systems", "Blockchain Technology"};
        return studies[faker.number().numberBetween(0, studies.length)];
    }

    private static String generateDegree() {
        // Generate a realistic degree title
        String[] degrees = {"BSc in Computer Science", "MSc in Data Science", "PhD in Physics", "MBA", "MA in Economics"};
        return degrees[faker.number().numberBetween(0, degrees.length)];
    }

    private static String generateProjectTitle() {
        // Generate a random project title
        String[] projects = {"AI for Healthcare", "Smart City Infrastructure", "Robotics in Manufacturing", "Data Analytics for Business", "Blockchain for Finance"};
        return projects[faker.number().numberBetween(0, projects.length)];
    }

    private static String generateUrl() {
        // Generate a realistic URL
        return "https://www." + faker.internet().domainName();
    }
}
