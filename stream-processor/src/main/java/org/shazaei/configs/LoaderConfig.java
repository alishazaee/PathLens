package org.shazaei.configs;

import java.util.HashMap;
import java.util.Map;

public class LoaderConfig {
    private String format;
    private Map<String, String> options;
    private String outputMode;

    public LoaderConfig(String format, Map<String, String> options, String outputMode) {
        this.format = format;
        this.options = options;
        this.outputMode = outputMode;
    }
    public String getOutputMode() {
        return outputMode;
    }
    public void setOutputMode(String outputMode) {
        this.outputMode = outputMode;
    }

    public  LoaderConfig() {}

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
