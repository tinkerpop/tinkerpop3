package com.tinkerpop.gremlin.neo4j.process.step.map;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Pieter Martin
 */
public class Neo4jGraphStep<E extends Element> extends GraphStep<E> {

    private final Neo4jGraph graph;
    public final List<HasContainer> hasContainers = new ArrayList<>();

    public Neo4jGraphStep(final Traversal traversal, final Class<E> returnClass, final Neo4jGraph graph) {
        super(traversal, returnClass);
        this.graph = graph;
    }

    public void generateTraverserIterator(final boolean trackPaths) {
        this.graph.tx().readWrite();
        this.starts.clear();
        if (trackPaths)
            this.starts.add(new TraverserIterator(this, Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
        else
            this.starts.add(new TraverserIterator(Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    public void clear() {
        this.starts.clear();
    }

    private Iterator<Edge> edges() {
        final HasContainer indexedContainer = getIndexKey(Edge.class);
        Stream<Edge> edgeStream = (indexedContainer != null) ?
                getEdgesUsingIndex(indexedContainer) :
                getEdges();
        return edgeStream.filter(e -> HasContainer.testAll((Edge) e, this.hasContainers)).iterator();
    }

    private Iterator<Vertex> vertices() {
        final HasContainer indexedContainer = getIndexKey(Vertex.class);
        Stream<Vertex> vertexStream = (indexedContainer != null) ?
                getVerticesUsingIndex(indexedContainer) :
                getVertices();
        return vertexStream.filter(v -> HasContainer.testAll((Vertex) v, this.hasContainers)).iterator();
    }

    private Stream<Vertex> getVertices() {
        return StreamFactory.stream(GlobalGraphOperations.at(this.graph.getRawGraph()).getAllNodes())
                .map(n -> new Neo4jVertex(n, this.graph));
    }

    private Stream<Edge> getEdges() {
        return StreamFactory.stream(GlobalGraphOperations.at(this.graph.getRawGraph()).getAllRelationships())
                .map(e -> new Neo4jEdge(e, this.graph));
    }

    private Stream<Vertex> getVerticesUsingIndex(final HasContainer indexedContainer) {
        if (indexedContainer.key.equals(Element.LABEL)) {
            ResourceIterator<Node> iterator = GlobalGraphOperations.at(graph.getRawGraph()).getAllNodesWithLabel(DynamicLabel.label((String) indexedContainer.value)).iterator();
            return StreamFactory.stream(iterator).map(n -> new Neo4jVertex(n, this.graph));
        } else if (indexedContainer.key.contains(":")) {
            String[] labelKey = indexedContainer.key.split(":");
            ResourceIterator<Node> iterator = graph.getRawGraph().findNodesByLabelAndProperty(DynamicLabel.label(labelKey[0]), labelKey[1], indexedContainer.value).iterator();
            return StreamFactory.stream(iterator).map(n -> new Neo4jVertex(n, this.graph));
        } else {
            final AutoIndexer indexer = this.graph.getRawGraph().index().getNodeAutoIndexer();
            if (indexer.isEnabled() && indexer.getAutoIndexedProperties().contains(indexedContainer.key))
                return StreamFactory.stream(this.graph.getRawGraph().index().getNodeAutoIndexer().getAutoIndex().get(indexedContainer.key, indexedContainer.value).iterator())
                        .map(n -> new Neo4jVertex(n, this.graph));
            else
                throw new IllegalStateException("Index not here"); // todo: unecessary check/throw?
        }
    }

    private Stream<Edge> getEdgesUsingIndex(final HasContainer indexedContainer) {
        final AutoIndexer indexer = this.graph.getRawGraph().index().getRelationshipAutoIndexer();
        if (indexer.isEnabled() && indexer.getAutoIndexedProperties().contains(indexedContainer.key))
            return StreamFactory.stream(this.graph.getRawGraph().index().getRelationshipAutoIndexer().getAutoIndex().get(indexedContainer.key, indexedContainer.value).iterator())
                    .map(e -> new Neo4jEdge(e, this.graph));
        else
            throw new IllegalStateException("Index not here"); // todo: unecessary check/throw?
    }

    private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
        final Set<String> indexedKeys;
        HasContainer indexedHasContainer = null;
        if (indexedClass.isAssignableFrom(Vertex.class)) {
            //Check for g.V().has(Element.LABEL, "xxx").has("key", "value") scenario
            if (this.hasContainers.size() > 1) {
                HasContainer hasContainer1 = this.hasContainers.get(0);
                HasContainer hasContainer2 = this.hasContainers.get(1);
                if (hasContainer1.key.equals(Element.LABEL) && hasContainer2.predicate.equals(Compare.EQUAL)) {
                    //In this case neo4j will work out if there is an index or not
                    indexedHasContainer = new HasContainer(hasContainer1.value + ":" + hasContainer2.key, Compare.EQUAL, hasContainer2.value);
                    this.hasContainers.remove(hasContainer1);
                    this.hasContainers.remove(hasContainer2);
                }
            }
            if (indexedHasContainer == null) {
                indexedKeys = new HashSet<>(this.graph.getRawGraph().index().getNodeAutoIndexer().getAutoIndexedProperties());
                this.graph.getRawGraph().schema().getIndexes().forEach(
                        indexDefinition -> indexDefinition.getPropertyKeys().forEach(
                                key -> indexedKeys.add(indexDefinition.getLabel().name() + ":" + key)
                        )
                );
                indexedHasContainer = this.hasContainers.stream()
                        .filter(c -> (indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL)) || (c.key.equals(Element.LABEL)))
                        .findFirst()
                        .orElseGet(() -> null);
                this.hasContainers.remove(indexedHasContainer);
            }
        } else if (indexedClass.isAssignableFrom(Edge.class)) {
            indexedKeys = this.graph.getRawGraph().index().getRelationshipAutoIndexer().getAutoIndexedProperties();
            indexedHasContainer = this.hasContainers.stream()
                    .filter(c -> (indexedKeys.contains(c.key) && c.predicate.equals(Compare.EQUAL)))
                    .findFirst()
                    .orElseGet(() -> null);

        } else
            throw new RuntimeException("Indexes must be related to a Vertex or an Edge");

        return indexedHasContainer;
    }

}
