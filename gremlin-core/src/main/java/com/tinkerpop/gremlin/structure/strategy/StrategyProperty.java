package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StrategyProperty<V> implements Property<V>, StrategyWrapped, WrappedProperty<Property<V>> {

    private final Property<V> innerProperty;
    private final StrategyContext<StrategyProperty<V>> strategyContext;
    private final StrategyGraph strategyGraph;
    private final GraphStrategy strategy;

    public StrategyProperty(final Property<V> innerProperty, final StrategyGraph strategyGraph) {
        this.innerProperty = innerProperty;
        this.strategyContext = new StrategyContext<>(strategyGraph, this);
        this.strategyGraph = strategyGraph;
        this.strategy = strategyGraph.getStrategy();
    }

    @Override
    public String key() {
        return this.strategyGraph.compose(
                s -> s.getPropertyKeyStrategy(this.strategyContext, strategy), getInnerProperty()::key).get();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.strategyGraph.compose(
                s -> s.<V>getPropertyValueStrategy(this.strategyContext, strategy), getInnerProperty()::value).get();
    }

    @Override
    public boolean isPresent() {
        return getInnerProperty().isPresent();
    }

    @Override
    public Element element() {
        final Element baseElement = getInnerProperty().element();
        return (baseElement instanceof Vertex ? new StrategyVertex((Vertex) baseElement, this.strategyGraph) :
                new StrategyEdge((Edge) baseElement, this.strategyGraph));
    }

    @Override
    public <E extends Throwable> V orElseThrow(final Supplier<? extends E> exceptionSupplier) throws E {
        return getInnerProperty().orElseThrow(exceptionSupplier);
    }

    @Override
    public V orElseGet(final Supplier<? extends V> valueSupplier) {
        return getInnerProperty().orElseGet(valueSupplier);
    }

    @Override
    public V orElse(final V otherValue) {
        return getInnerProperty().orElse(otherValue);
    }

    @Override
    public void ifPresent(final Consumer<? super V> consumer) {
        getInnerProperty().ifPresent(consumer);
    }

    @Override
    public void remove() {
        this.strategyGraph.compose(
                s -> s.getRemovePropertyStrategy(strategyContext, strategy),
                () -> {
                    getInnerProperty().remove();
                    return null;
                }).get();
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyPropertyString(this);
    }

    @Override
    public Property<V> getBaseProperty() {
        if (getInnerProperty() instanceof StrategyWrapped)
            return ((StrategyProperty)getInnerProperty()).getBaseProperty();
        else
            return getInnerProperty();
    }

    public Property<V> getInnerProperty() {
        return this.innerProperty;
    }
}
