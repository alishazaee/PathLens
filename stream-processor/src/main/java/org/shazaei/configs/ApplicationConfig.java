package org.shazaei.configs;

public class ApplicationConfig {
    private ExtractorConfig extractorConfig;
    private LoaderConfig loaderConfig;
    public TransformConfig transformConfig;
    public  ApplicationConfig(ExtractorConfig extractorConfig,
                              LoaderConfig loaderConfig,
                              TransformConfig transformConfig) {
        this.extractorConfig = extractorConfig;
        this.loaderConfig = loaderConfig;
        this.transformConfig = transformConfig;
    }
    public ApplicationConfig() {}

    public ExtractorConfig getExtractorConfig() {
        return extractorConfig;
    }

    public void setExtractorConfig(ExtractorConfig extractorConfig) {
        this.extractorConfig = extractorConfig;
    }

    public LoaderConfig getLoaderConfig() {
        return loaderConfig;
    }

    public void setLoaderConfig(LoaderConfig loaderConfig) {
        this.loaderConfig = loaderConfig;
    }

    public TransformConfig getTransformConfig() {
        return transformConfig;
    }

    public void setTransformConfig(TransformConfig transformConfig) {
        this.transformConfig = transformConfig;
    }
}
