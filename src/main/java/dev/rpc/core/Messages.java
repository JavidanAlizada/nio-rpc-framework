package dev.rpc.core;

/** Error messages have to fit the frame's string limit: 65,535 UTF-8 bytes. */
final class Messages {

    static final int MAX_BYTES = 0xFFFF;
    private static final String ELLIPSIS = "…"; // 3 bytes in UTF-8

    private Messages() {
    }

    /** Cuts message at a code point boundary so it fits, ending it with "…". null becomes "". */
    static String truncate(String message) {
        if (message == null) {
            return "";
        }
        if (message.length() * 3 <= MAX_BYTES) {
            return message; // a UTF-16 char never takes more than 3 UTF-8 bytes
        }
        int budget = MAX_BYTES - 3;
        int bytes = 0;
        int i = 0;
        while (i < message.length()) {
            int cp = message.codePointAt(i);
            int size = cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
            if (bytes + size > budget) {
                return message.substring(0, i) + ELLIPSIS;
            }
            bytes += size;
            i += Character.charCount(cp);
        }
        return message;
    }
}
