package org.github.seonwkim

import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.domain.JavaField
import com.tngtech.archunit.core.domain.JavaMethod
import com.tngtech.archunit.core.domain.JavaType
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.lang.ArchCondition
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.ConditionEvents
import com.tngtech.archunit.lang.SimpleConditionEvent
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition
import org.github.seonwkim.common.TimestampedByteArray
import kotlin.test.Test

class ProjectRulesTest {
    private val classes = ClassFileImporter()
        .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
        .importPackages("org.github.seonwkim")

    private val allowedClasses = listOf(TimestampedByteArray::class.java.name)

    @Test
    fun `only allowed classes can access List of type Byte`() {
        val rule: ArchRule = ArchRuleDefinition
            .classes()
            .should(onlyAllowedClassesCanAccessListOfTypeByte(allowedClasses))

        rule.check(classes)
    }

    private fun onlyAllowedClassesCanAccessListOfTypeByte(allowedClasses: List<String>): ArchCondition<JavaClass> =
        object : ArchCondition<JavaClass>("access List<Byte>") {
            override fun check(item: JavaClass, events: ConditionEvents) {
                item.fields.forEach { field ->
                    if (allowedClasses.contains(item.name)) return@forEach
                    if (field.isListOfByte()) {
                        events.add(
                            SimpleConditionEvent.violated(
                                field,
                                "${item.name} uses List<Byte> in field ${field.name}"
                            )
                        )
                    }
                }
                item.methods.forEach { method ->
                    if (allowedClasses.contains(item.name)) return@forEach
                    if (method.isUsingListOfByte()) {
                        events.add(
                            SimpleConditionEvent.violated(
                                method,
                                "${item.name} uses List<Byte> in method ${method.name}"
                            )
                        )
                    }
                }
            }
        }

    private fun JavaField.isListOfByte(): Boolean {
        return this.type.isListOfByteType()
    }

    private fun JavaMethod.isUsingListOfByte(): Boolean {
        return this.parameterTypes.any { it.isListOfByteType() } ||
                this.returnType.isListOfByteType()
    }

    private fun JavaType.isListOfByteType(): Boolean {
        return this.allInvolvedRawTypes.map { it.name }
            .containsAll(listOf("java.util.List", "java.lang.Byte"))
    }
}
