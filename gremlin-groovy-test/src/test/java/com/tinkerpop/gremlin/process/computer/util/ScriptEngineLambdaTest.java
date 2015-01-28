package com.tinkerpop.gremlin.process.computer.util;

import java.util.HashSet;
import java.util.Set;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Matt Frantz (matthew.h.frantz+tinkerpop@gmail.com)
 */
public class ScriptEngineLambdaTest {

    private final static String GROOVY_SCRIPT_ENGINE_NAME = "Groovy";

    // Function.apply

    @Test
    public void simpleFunctionWorks() {
        final ScriptEngineLambda lambda = newGroovyLambda("a + 2");
        assertEquals(lambda.apply(5), 7);
        assertEquals(lambda.apply("foo"), "foo2");
    }

    // Supplier.get

    @Test
    public void simpleSupplierWorks() {
        final ScriptEngineLambda lambda = newGroovyLambda("System.currentTimeMillis()");
        assertNotEquals(lambda.get(), 0);
    }

    // Consumer.accept

    @Test
    public void simpleConsumerWorks() {
        final Set set = pokeSet();
        final ScriptEngineLambda lambda = newGroovyLambda("set.add(a)");

        lambda.accept(1);
        lambda.accept(2);
        lambda.accept(3);

        assertEquals(set.size(), 3);
    }

    // BiConsumer.accept

    @Test
    public void simpleBiConsumerWorks() {
        final Set set = pokeSet();
        final ScriptEngineLambda lambda = newGroovyLambda("set.add([a, b])");

        lambda.accept(1, 2);
        lambda.accept(2, 3);
        lambda.accept(3, 4);

        assertEquals(set.size(), 3);
    }

    // TriConsumer.accept

    @Test
    public void simpleTriConsumerWorks() {
        final Set set = pokeSet();
        final ScriptEngineLambda lambda = newGroovyLambda("set.add([a, b, c])");

        lambda.accept(1, 2, 3);
        lambda.accept(2, 3, 4);
        lambda.accept(3, 4, 5);

        assertEquals(set.size(), 3);
    }

    // Predicate.test

    @Test
    public void trivialGroovyPredicateWorks() {
        final ScriptEngineLambda lambda = newGroovyLambda("true");
        assertTrue(lambda.test("foo"));
    }

    @Test
    public void oneArgGroovyPredicateWorks() {
        final ScriptEngineLambda lambda = newGroovyLambda("a < 100");
        assertTrue(lambda.test(0));
        assertTrue(lambda.test(99));
        assertFalse(lambda.test(100));
    }

    @Test
    public void trivialGroovyFunctionWorks() {
        final ScriptEngineLambda lambda = newGroovyLambda("2 + 2");
        assertEquals(lambda.apply("foo"), 4);
    }

    @Test
    public void oneArgGroovyFunctionWorks() {
        final ScriptEngineLambda lambda = newGroovyLambda("a + 2");
        assertEquals(lambda.apply(3), 5);
        assertEquals(lambda.apply(10), 12);
        assertEquals(lambda.apply("foo"), "foo2");
    }

    // Utilities

    /**
     * Bind a set named "set" in the Groovy engine.
     */
    private static Set pokeSet() {
        final ScriptEngine engine = getEngine();
        final Bindings bindings = engine.createBindings();
        final Set set = new HashSet();
        bindings.put("set", set);
        // Set the global bindings (since ScriptEngineLambda sets the engine bindings on each invocation).
        engine.setBindings(bindings, ScriptContext.GLOBAL_SCOPE);
        return set;
    }

    /**
     * Return the engine used by ScriptEngineLambda in these tests.
     */
    private static ScriptEngine getEngine() {
        return ScriptEngineCache.get(GROOVY_SCRIPT_ENGINE_NAME);
    }

    /**
     * Create a ScriptEngineLambda that will accept Groovy.
     */
    private static ScriptEngineLambda newGroovyLambda(String groovy) {
        return new ScriptEngineLambda(GROOVY_SCRIPT_ENGINE_NAME, groovy);
    }
}
