package co.ke.CoreNexus.db_utils.db.utils;

import java.util.Random;

/**
 * mock_data_scedar (co.ke.CoreNexus.db_utils.db.utils)
 * Created by: oloo
 * On: 11/11/2024. 23:13
 * Description:
 **/
public class Randomizer {

    private static final Random RANDOM = new Random();

    // Method to generate a random alphanumeric string of specified length
    public static String generateRandomAlphanumeric(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuilder randomString = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int index = RANDOM.nextInt(characters.length());
            randomString.append(characters.charAt(index));
        }

        return randomString.toString();
    }
}