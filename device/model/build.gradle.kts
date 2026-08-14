plugins {
    id("java-conventions")
    id("java-library")
}

dependencies {
    api(libs.jakartaValidation)
    api(libs.commonsLang)
}