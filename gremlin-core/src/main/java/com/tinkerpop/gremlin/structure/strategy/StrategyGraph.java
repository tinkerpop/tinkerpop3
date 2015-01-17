package com.tinkerpop.gremlin.structure.strategy;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * A wrapper class for {@link Graph} instances that host and apply a {@link GraphStrategy}.  The wrapper implements
 * {@link Graph} itself and intercepts calls made to the hosted instance and then applies the strategy.  Methods
 * that return an extension of {@link com.tinkerpop.gremlin.structure.Element} or a
 * {@link com.tinkerpop.gremlin.structure.Property} will be automatically wrapped in a {@link StrategyWrapped}
 * implementation.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StrategyGraph implements Graph, Graph.Iterators, StrategyWrapped, WrappedGraph<Graph> {
    private final Graph innerGraph;
    private final GraphStrategy strategy;
    private final StrategyContext<StrategyGraph> graphContext;

    public StrategyGraph(final Graph innerGraph) {
        this(innerGraph, IdentityStrategy.instance());
    }

    public StrategyGraph(final Graph innerGraph, final GraphStrategy strategy) {
        if (null == strategy) throw new IllegalArgumentException("Strategy cannot be null");

        this.strategy = strategy;
        this.innerGraph = innerGraph;
        this.graphContext = new StrategyContext<>(this, this);
    }

    /**
     * Gets the underlying base {@link Graph} that is being hosted within this wrapper.
     */
    @Override
    public Graph getBaseGraph() {
        if (getInnerGraph() instanceof StrategyWrapped)
            return ((StrategyGraph)getInnerGraph()).getBaseGraph();
        else
            return getInnerGraph();
    }

    public Graph getInnerGraph() {
        return this.innerGraph;
    }


    /**
     * Gets the {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy} for the {@link com.tinkerpop.gremlin.structure.Graph}.
     */
    public GraphStrategy getStrategy() {
        return this.strategy;
    }

    /**
     * Return a {@link GraphStrategy} function that takes the base function of the form denoted by {@code T} as
     * an argument and returns back a function with {@code T}.
     *
     * @param f    a function to execute if a {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy}.
     * @param impl the base implementation of an operation.
     * @return a function that will be applied in the Gremlin Structure implementation
     */
    public <T> T compose(final Function<GraphStrategy, UnaryOperator<T>> f, final T impl) {
        return f.apply(this.strategy).apply(impl);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        final Optional<Vertex> v = Optional.ofNullable(compose(
                s -> s.getAddVertexStrategy(this.graphContext, strategy),
                getInnerGraph()::addVertex).apply(keyValues));
        return v.isPresent() ? new StrategyVertex(v.get(), this) : null;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V(final Object... vertexIds) {
        return this.compose(s -> s.getGraphVStrategy(this.graphContext, this.strategy), getInnerGraph()::V).apply(vertexIds).map(vertex -> new StrategyVertex(vertex.get(), this));
       /* final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(this.getClass());
        return traversal.asAdmin().addStep(new StrategyGraphStep<>(traversal, this, Vertex.class, this.compose(
                s -> s.getGraphVStrategy(this.graphContext, strategy),
<<<<<<< HEAD
                getInnerGraph()::V).apply(vertexIds), this);
=======
                this.baseGraph::V).apply(vertexIds))); */
//>>>>>>> upstream/master
    }

    @Override
    public GraphTraversal<Edge, Edge> E(final Object... edgeIds) {
        return this.compose(s -> s.getGraphEStrategy(this.graphContext, this.strategy), getInnerGraph()::E).apply(edgeIds).map(edge -> new StrategyEdge(edge.get(), this));
        /*final GraphTraversal<Edge, Edge> traversal = new DefaultGraphTraversal<>(this.getClass());
        return traversal.asAdmin().addStep(new StrategyGraphStep<>(traversal, this, Edge.class, this.compose(
                s -> s.getGraphEStrategy(this.graphContext, strategy),
<<<<<<< HEAD
                getInnerGraph()::E).apply(edgeIds), this);
    }

    @Override
    public <S> GraphTraversal<S, S> of() {
        return new StrategyTraversal<>(this);
=======
                this.baseGraph::E).apply(edgeIds)));*/
//>>>>>>> upstream/master
    }

    @Override
    public <T extends Traversal<S, S>, S> T of(final Class<T> traversalClass) {
        return getInnerGraph().of(traversalClass);  // TODO: wrap the users traversal in StrategyWrappedTraversal
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        return getInnerGraph().compute(graphComputerClass);
    }

    @Override
    public Transaction tx() {
        return getInnerGraph().tx();
    }

    @Override
    public Variables variables() {
        return new StrategyVariables(getInnerGraph().variables(), this);
    }

    @Override
    public Configuration configuration() {
        return getInnerGraph().configuration();
    }

    @Override
    public Features features() {
        return getInnerGraph().features();
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        return new StrategyVertex.StrategyVertexIterator(compose(s -> s.getGraphIteratorsVertexIteratorStrategy(this.graphContext, strategy), getInnerGraph().iterators()::vertexIterator).apply(vertexIds), this);
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        return new StrategyEdge.StrategyEdgeIterator(compose(s -> s.getGraphIteratorsEdgeIteratorStrategy(this.graphContext, strategy), getInnerGraph().iterators()::edgeIterator).apply(edgeIds), this);
    }

    @Override
    public void close() throws Exception {
        // compose function doesn't seem to want to work here even though it works with other Supplier<Void>
        // strategy functions. maybe the "throws Exception" is hosing it up.......
        this.strategy.getGraphCloseStrategy(this.graphContext, strategy).apply(FunctionUtils.wrapSupplier(() -> {
            getInnerGraph().close();
            return null;
        })).get();
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(strategy, getInnerGraph());
    }

    public static class Exceptions {
        public static IllegalStateException strategyGraphIsSafe() {
            return new IllegalStateException("StrategyGraph is in safe mode - its elements cannot be unwrapped.");
        }
    }


}
