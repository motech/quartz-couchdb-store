package org.motechproject.quartz;

import java.util.UUID;

public class IdRandomizer {

    public static String id(String name) {
        return name + "-" + UUID.randomUUID();
    }
}
