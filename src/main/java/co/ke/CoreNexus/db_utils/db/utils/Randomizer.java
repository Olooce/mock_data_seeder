package co.ke.CoreNexus.db_utils.db.utils;

import java.util.Random;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.db.utils)
 * Created by: oloo
 * On: 11/11/2024. 23:13
 * Description:
 **/
public class Randomizer {

    // Method to generate a random alphanumeric string of specified length
    public static String generateRandomAlphanumeric(int length) {
        // Define the character set (A-Z, 0-9)
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuilder randomString = new StringBuilder(length);

        // Generate the random string
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            randomString.append(characters.charAt(index));
        }

        return randomString.toString();
    }
}