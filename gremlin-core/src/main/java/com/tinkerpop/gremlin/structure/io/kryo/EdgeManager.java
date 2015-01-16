package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface EdgeManager {
    public void keepReadEdge(final Edge e, final Kryo kryo);
    public default void edgeReadingComplete(final Kryo kryo) {}
    public void writeAllEdges(final Graph g, final Kryo kryo) throws IOException;

    public static class SpillOverEdgeManager implements EdgeManager {
        private static final double DEFAULT_SPILLOVER = 0.5d;
        private static final long DEFAULT_UNTIL_SPILLOVER = 10000;
        private final InMemoryEdgeManager inMemory;
        private final FileSystemEdgeManager fileSystem;
        private final double spilloverAt;
        private long edgeCounter = 0;
        private final long untilSpillOver;
        private boolean lastCheck = true;

        public SpillOverEdgeManager() throws IOException {
            this(System.getProperty("java.io.tmpdir"));
        }

        public SpillOverEdgeManager(final String workingDirectory) throws IOException {
            this(workingDirectory, DEFAULT_SPILLOVER, DEFAULT_UNTIL_SPILLOVER);
        }

        public SpillOverEdgeManager(final String workingDirectory, final double spillOverAt, final long untilSpillOver) throws IOException {
            this.inMemory = new InMemoryEdgeManager();
            this.fileSystem = new FileSystemEdgeManager(workingDirectory);
            this.spilloverAt = spillOverAt;
            this.untilSpillOver = untilSpillOver;
        }

        @Override
        public void edgeReadingComplete(final Kryo kryo) {
            inMemory.edgeReadingComplete(kryo);
            fileSystem.edgeReadingComplete(kryo);
        }

        @Override
        public void keepReadEdge(final Edge e, final Kryo kryo) {
            if (useMemoryManager())
                inMemory.keepReadEdge(e, kryo);
            else
                fileSystem.keepReadEdge(e, kryo);

            edgeCounter++;
        }

        @Override
        public void writeAllEdges(final Graph g, final Kryo kryo) throws IOException {
            inMemory.writeAllEdges(g, kryo);
            fileSystem.writeAllEdges(g, kryo);
        }

        private boolean useMemoryManager() {
            if (edgeCounter > untilSpillOver && edgeCounter % 1000 == 0) {
                final Runtime r = Runtime.getRuntime();
                final long free = r.freeMemory();
                final long max = r.maxMemory();
                final long used = max - free;
                final double usedPercentage = (double) used / (double) max;

                // if the amount used is less than the spillover then continue to use memory, otherwise use
                // file system
                lastCheck = usedPercentage < spilloverAt;
            }

            return lastCheck;
        }
    }

    public static class InMemoryEdgeManager implements EdgeManager {

        private List<Edge> edges;

        public InMemoryEdgeManager() {
            this(new ArrayList<>());
        }

        public InMemoryEdgeManager(final List<Edge> edges) {
            this.edges = edges;
        }

        @Override
        public void keepReadEdge(final Edge e, final Kryo kryo) {
            edges.add(e);
        }

        @Override
        public void writeAllEdges(final Graph g, final Kryo kryo) throws IOException {
            System.out.println("EDGES: " + edges.size());
            edges.forEach(e -> {
                final List<Object> edgeArgs = new ArrayList<>();
                final DetachedEdge detachedEdge = (DetachedEdge) e;
                final Vertex vOut = g.iterators().vertexIterator(detachedEdge.iterators().vertexIterator(Direction.OUT).next().id()).next();
                final Vertex inV = g.iterators().vertexIterator(detachedEdge.iterators().vertexIterator(Direction.IN).next().id()).next();

                detachedEdge.iterators().propertyIterator().forEachRemaining(p -> edgeArgs.addAll(Arrays.asList(p.key(), p.value())));

                appendToArgList(edgeArgs, T.id, detachedEdge.id());

                vOut.addEdge(detachedEdge.label(), inV, edgeArgs.toArray());
            });
        }
    }

    public static class FileSystemEdgeManager implements EdgeManager {
        private final File tempFile;
        private final Output output;

        public FileSystemEdgeManager() throws IOException {
            this(System.getProperty("java.io.tmpdir"));
        }

        public FileSystemEdgeManager(final String workingDirectory) throws IOException {
            final File f = new File(workingDirectory + File.separator + UUID.randomUUID() + ".tmp");
            if (!f.exists()) f.getParentFile().mkdirs();

            this.tempFile = f;
            output = new Output(new FileOutputStream(tempFile));
        }

        @Override
        public void keepReadEdge(final Edge e, final Kryo kryo) {
            kryo.writeClassAndObject(output, e);
        }

        @Override
        public void edgeReadingComplete(final Kryo kryo) {
            kryo.writeClassAndObject(output, EdgeTerminator.INSTANCE);
            kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
        }

        @Override
        public void writeAllEdges(final Graph g, final Kryo kryo) throws IOException {
            output.close();

            final Input input = new Input(new FileInputStream(tempFile));
            while (!input.eof()) {
                // in this case the outId is the id assigned by the graph
                Object next = kryo.readClassAndObject(input);
                while (!next.equals(EdgeTerminator.INSTANCE)) {
                    final List<Object> edgeArgs = new ArrayList<>();
                    final DetachedEdge detachedEdge = (DetachedEdge) next;
                    final Vertex vOut = g.iterators().vertexIterator(detachedEdge.iterators().vertexIterator(Direction.OUT).next().id()).next();
                    final Vertex inV = g.iterators().vertexIterator(detachedEdge.iterators().vertexIterator(Direction.IN).next().id()).next();

                    detachedEdge.iterators().propertyIterator().forEachRemaining(p -> edgeArgs.addAll(Arrays.asList(p.key(), p.value())));

                    appendToArgList(edgeArgs, T.id, detachedEdge.id());

                    vOut.addEdge(detachedEdge.label(), inV, edgeArgs.toArray());

                    next = kryo.readClassAndObject(input);
                }

                // vertex terminator
                kryo.readClassAndObject(input);
            }

            deleteTempFileSilently();
        }

        private void deleteTempFileSilently() {
            try {
                tempFile.delete();
            } catch (Exception ignored) {
            }
        }
    }

    static void appendToArgList(final List<Object> propertyArgs, final Object key, final Object val) {
        propertyArgs.add(key);
        propertyArgs.add(val);
    }
}
