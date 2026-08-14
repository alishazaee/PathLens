plugins {
    id("java-conventions")
    id("java-library")
    id("junit5-conventions")
}

dependencies {

    api(project(":device:model"))
    api(project(":common-libs:common-model"))
    api(libs.bundles.jerseyClient)
    api(libs.jackson.datatype.jsr310)
}
