import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    id("java-conventions")
    id("junit5-conventions")
    id("org.springframework.boot")
    id("jooq-codegen")
}

extra["jooqPackageName"] = "ir.pathlens.device.rest.db"

dependencies {
    // Imports Spring Boot maven BOM
    implementation(platform(SpringBootPlugin.BOM_COORDINATES))

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-jooq")
    implementation("org.springframework.data:spring-data-commons")

    annotationProcessor(libs.lombok)
    compileOnly(libs.lombok)

    // Todo try to remove it with api in gradle
    implementation(project(":device:model"))
    implementation(project(":common-libs:common-model"))

    implementation(libs.guava)
    implementation(libs.springDocWebmvc)
    implementation("io.micrometer:micrometer-registry-prometheus")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation(project(":device:client"))
    testImplementation(project(":device:cache"))
    testImplementation(project(":common-libs:test-extensions"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.2")
    testCompileOnly("org.projectlombok:lombok:1.18.32")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.32")
    testImplementation(libs.testcontainersPostgresql)
    testImplementation(libs.testcontainersJupiter)
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-Xlint:-processing")
}
