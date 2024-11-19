package org.seonWKim

import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.domain.JavaField
import com.tngtech.archunit.core.domain.JavaMethod
import com.tngtech.archunit.core.domain.JavaType
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.lang.ArchCondition
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.ConditionEvents
import com.tngtech.archunit.lang.SimpleConditionEvent
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition
import kotlin.test.Test

class ProjectRulesTest {
    private val classes = ClassFileImporter().importPackages("org.seonWKim")

    @Test
    fun `no classes should use List of Byte`() {
        val rule: ArchRule = ArchRuleDefinition.noClasses()
            .should(accessListOfByte())

        rule.check(classes)
    }

    private fun accessListOfByte(): ArchCondition<JavaClass> =
        object : ArchCondition<JavaClass>("access List<Byte>") {
            override fun check(item: JavaClass, events: ConditionEvents) {
                item.fields.forEach { field ->
                    if (field.isListOfByte()) {
                        events.add(SimpleConditionEvent.violated(field, "${item.name} uses List<Byte> in field ${field.name}"))
                    }
                }
                item.methods.forEach { method ->
                    if (method.isUsingListOfByte()) {
                        events.add(SimpleConditionEvent.violated(method, "${item.name} uses List<Byte> in method ${method.name}"))
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
        return this.name == "java.util.List"
    }
}
