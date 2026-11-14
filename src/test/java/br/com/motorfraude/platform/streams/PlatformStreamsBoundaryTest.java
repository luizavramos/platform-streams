package br.com.motorfraude.platform.streams;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PlatformStreamsBoundaryTest {

    private static final String BASE = "br.com.motorfraude.platform.streams";

    private static com.tngtech.archunit.core.domain.JavaClasses main() {
        return new ClassFileImporter().withImportOption(new ImportOption.DoNotIncludeTests())
                .withImportOption(new ImportOption.DoNotIncludeJars()).importPackages(BASE);
    }

    @Test
    @DisplayName("o código principal não usa Spring: o servidor HTTP é o do JDK")
    void semSpring() {
        noClasses().should().dependOnClassesThat().resideInAnyPackage("org.springframework..", "jakarta.servlet..")
                .because("platform-streams serve apps Kafka Streams sem Spring (plano 003)").check(main());
    }

    @Test
    @DisplayName("o nosso código nunca importa o Jackson 2, que só está no classpath de execução por causa do kafka-streams")
    void semJackson2() {
        noClasses().should().dependOnClassesThat().resideInAnyPackage("com.fasterxml.jackson.databind..", "com.fasterxml.jackson.core..")
                .check(main());
    }
}
