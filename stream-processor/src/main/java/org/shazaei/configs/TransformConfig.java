package org.shazaei.configs;

public class TransformConfig {
    private String windowSize;
    private int Precision;

    public TransformConfig(String windowSize) {
        this.windowSize = windowSize;
    }

    public TransformConfig() {}

    public String getWindowSize() {
        return windowSize;
    }
    public void setWindowSize(String windowSize) {
        this.windowSize = windowSize;
    }

    public int getPrecision() {
        return Precision;
    }
    public void setPrecision(int precision) {
        Precision = precision;
    }
}
