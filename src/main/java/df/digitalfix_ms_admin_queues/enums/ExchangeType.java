package df.digitalfix_ms_admin_queues.enums;

import lombok.Getter;

@Getter
public enum ExchangeType {
    DIRECT("direct"),
    TOPIC("topic"),
    FANOUT("fanout"),
    HEADERS("headers");

    private final String value;

    ExchangeType(String value) {
        this.value = value;
    }

    public static boolean isValid(String type) {
        for (ExchangeType et : values()) {
            if (et.getValue().equalsIgnoreCase(type)) {
                return true;
            }
        }
        return false;
    }
}
