plugins {
    id("java-conventions")
    id("java-library")
    id("junit5-conventions")
    id("java-test-fixtures")
}

dependencies {
    api(project(":logs-proto:raw-log:proto"))
    implementation(project(":logs-proto:camera:parser"))
}
