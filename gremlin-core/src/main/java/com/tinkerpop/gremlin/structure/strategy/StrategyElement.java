package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class StrategyElement implements Element, StrategyWrapped {
    protected final StrategyGraph strategyGraph;
    protected final GraphStrategy strategy;
    protected final Element innerElement;
    protected final StrategyContext<StrategyElement> elementStrategyContext;

    protected StrategyElement(final Element innerElement, final StrategyGraph strategyGraph) {
        this.strategyGraph = strategyGraph;
        this.strategy = strategyGraph.getStrategy();
        this.innerElement = innerElement;
        this.elementStrategyContext = new StrategyContext<>(strategyGraph, this);
    }

    public Element getBaseElement() {
        if (this.innerElement instanceof StrategyWrapped)
            return ((StrategyElement)getInnerElement()).getBaseElement();
        else
            return this.innerElement;
    }

    public Element getInnerElement() {
        return this.innerElement;
    }

    @Override
    public int hashCode() {
        return this.innerElement.hashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
}
