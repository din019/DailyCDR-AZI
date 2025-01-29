package org.example.utilities;

public class StringUtils {
    // Helper method to ensure the message length does not exceed the limit
    public static String ensureNotOutOfRange(String message, int maxLength) {
        if (message == null) return null; // Handle null messages
        return message.length() > maxLength ? message.substring(0, maxLength) : message;
    }
}
