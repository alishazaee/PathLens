package org.shazaei.configs;

import java.util.HashMap;
import java.util.Map;

public class ExtractorConfig {
    private String format;
    private Map<String, String> options = new HashMap<>();

    public ExtractorConfig(String format, Map<String, String> options) {
        this.format = format;
        this.options = options;
    }

    public ExtractorConfig() {}

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

}
