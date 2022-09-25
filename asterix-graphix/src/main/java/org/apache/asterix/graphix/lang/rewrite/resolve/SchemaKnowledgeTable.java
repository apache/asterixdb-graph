/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.lang.rewrite.resolve;

import static org.apache.asterix.graphix.extension.GraphixMetadataExtension.getGraph;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.common.metadata.VertexIdentifier;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.rewrite.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.metadata.entity.schema.Edge;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

/**
 * A collection of ground truths, derived from the graph schema (either a {@link Graph} or {@link GraphConstructor}).
 */
public class SchemaKnowledgeTable implements Iterable<SchemaKnowledgeTable.KnowledgeRecord> {
    private final Set<KnowledgeRecord> knowledgeRecordSet = new HashSet<>();
    private final Set<VertexIdentifier> vertexIdentifierSet = new HashSet<>();
    private final Set<EdgeIdentifier> edgeIdentifierSet = new HashSet<>();
    private final GraphIdentifier graphIdentifier;

    public SchemaKnowledgeTable(FromGraphClause fromGraphClause, GraphixRewritingContext graphixRewritingContext)
            throws CompilationException {
        MetadataProvider metadataProvider = graphixRewritingContext.getMetadataProvider();
        DataverseName dataverseName = (fromGraphClause.getDataverseName() == null)
                ? metadataProvider.getDefaultDataverseName() : fromGraphClause.getDataverseName();
        graphIdentifier = fromGraphClause.getGraphIdentifier(metadataProvider);

        // Establish our schema knowledge.
        GraphConstructor graphConstructor = fromGraphClause.getGraphConstructor();
        if (graphConstructor == null) {
            Identifier graphName = fromGraphClause.getGraphName();

            // First, try to find our graph inside our declared graph set.
            Map<GraphIdentifier, DeclareGraphStatement> declaredGraphs = graphixRewritingContext.getDeclaredGraphs();
            DeclareGraphStatement declaredGraph = declaredGraphs.get(graphIdentifier);
            if (declaredGraph != null) {
                initializeWithGraphConstructor(declaredGraph.getGraphConstructor());

            } else {
                // Otherwise, fetch the graph from our metadata.
                try {
                    MetadataTransactionContext metadataTxnContext = metadataProvider.getMetadataTxnContext();
                    Graph graphFromMetadata = getGraph(metadataTxnContext, dataverseName, graphName.getValue());
                    if (graphFromMetadata == null) {
                        throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                                "Graph " + graphName.getValue() + " does not exist.");
                    }
                    initializeWithMetadataGraph(graphFromMetadata);

                } catch (AlgebricksException e) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, fromGraphClause.getSourceLocation(),
                            "Graph " + graphName.getValue() + " does not exist.");
                }
            }

        } else {
            initializeWithGraphConstructor(graphConstructor);
        }
    }

    private void initializeWithMetadataGraph(Graph graph) {
        for (Vertex vertex : graph.getGraphSchema().getVertices()) {
            VertexIdentifier vertexIdentifier = vertex.getIdentifier();
            vertexIdentifierSet.add(vertexIdentifier);
        }
        for (Edge edge : graph.getGraphSchema().getEdges()) {
            ElementLabel sourceLabel = edge.getSourceLabel();
            ElementLabel edgeLabel = edge.getLabel();
            ElementLabel destLabel = edge.getDestinationLabel();

            // Build our source and destination vertex identifiers.
            VertexIdentifier sourceIdentifier = new VertexIdentifier(graphIdentifier, sourceLabel);
            VertexIdentifier destIdentifier = new VertexIdentifier(graphIdentifier, destLabel);
            vertexIdentifierSet.add(sourceIdentifier);
            vertexIdentifierSet.add(destIdentifier);
            edgeIdentifierSet.add(edge.getIdentifier());

            // Update our knowledge set.
            knowledgeRecordSet.add(new KnowledgeRecord(sourceLabel, edgeLabel, destLabel));
        }
    }

    private void initializeWithGraphConstructor(GraphConstructor graphConstructor) {
        for (GraphConstructor.VertexConstructor vertexElement : graphConstructor.getVertexElements()) {
            VertexIdentifier vertexIdentifier = new VertexIdentifier(graphIdentifier, vertexElement.getLabel());
            vertexIdentifierSet.add(vertexIdentifier);
        }
        for (GraphConstructor.EdgeConstructor edgeElement : graphConstructor.getEdgeElements()) {
            ElementLabel sourceLabel = edgeElement.getSourceLabel();
            ElementLabel edgeLabel = edgeElement.getEdgeLabel();
            ElementLabel destLabel = edgeElement.getDestinationLabel();

            // Build our edge identifiers, and source & destination vertex identifiers.
            VertexIdentifier sourceIdentifier = new VertexIdentifier(graphIdentifier, sourceLabel);
            VertexIdentifier destIdentifier = new VertexIdentifier(graphIdentifier, destLabel);
            EdgeIdentifier edgeIdentifier = new EdgeIdentifier(graphIdentifier, sourceLabel, edgeLabel, destLabel);
            vertexIdentifierSet.add(sourceIdentifier);
            vertexIdentifierSet.add(destIdentifier);
            edgeIdentifierSet.add(edgeIdentifier);

            // Update our knowledge set.
            knowledgeRecordSet.add(new KnowledgeRecord(sourceLabel, edgeLabel, destLabel));
        }
    }

    public Set<ElementLabel> getVertexLabels() {
        return vertexIdentifierSet.stream().map(VertexIdentifier::getVertexLabel).collect(Collectors.toSet());
    }

    public Set<ElementLabel> getEdgeLabels() {
        return edgeIdentifierSet.stream().map(EdgeIdentifier::getEdgeLabel).collect(Collectors.toSet());
    }

    public boolean isValidEdge(EdgePatternExpr edgePatternExpr) {
        for (ElementLabel leftLabel : edgePatternExpr.getLeftVertex().getLabels()) {
            for (ElementLabel rightLabel : edgePatternExpr.getRightVertex().getLabels()) {
                EdgeDescriptor edgeDescriptor = edgePatternExpr.getEdgeDescriptor();
                for (ElementLabel edgeLabel : edgeDescriptor.getEdgeLabels()) {
                    if (edgeDescriptor.getEdgeDirection() != EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT) {
                        EdgeIdentifier id = new EdgeIdentifier(graphIdentifier, leftLabel, edgeLabel, rightLabel);
                        if (!edgeIdentifierSet.contains(id)) {
                            return false;
                        }
                    }
                    if (edgeDescriptor.getEdgeDirection() != EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT) {
                        EdgeIdentifier id = new EdgeIdentifier(graphIdentifier, rightLabel, edgeLabel, leftLabel);
                        if (!edgeIdentifierSet.contains(id)) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    @Override
    public Iterator<KnowledgeRecord> iterator() {
        return knowledgeRecordSet.iterator();
    }

    public static class KnowledgeRecord {
        private final ElementLabel sourceElementLabel;
        private final ElementLabel destElementLabel;
        private final ElementLabel edgeLabel;

        public KnowledgeRecord(ElementLabel sourceElementLabel, ElementLabel edgeLabel, ElementLabel destElementLabel) {
            this.sourceElementLabel = Objects.requireNonNull(sourceElementLabel);
            this.destElementLabel = Objects.requireNonNull(destElementLabel);
            this.edgeLabel = Objects.requireNonNull(edgeLabel);
        }

        public ElementLabel getSourceVertexLabel() {
            return sourceElementLabel;
        }

        public ElementLabel getDestVertexLabel() {
            return destElementLabel;
        }

        public ElementLabel getEdgeLabel() {
            return edgeLabel;
        }

        @Override
        public String toString() {
            return String.format("(%s)-[%s]->(%s)", sourceElementLabel, edgeLabel, destElementLabel);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof KnowledgeRecord)) {
                return false;
            }
            KnowledgeRecord target = (KnowledgeRecord) object;
            return sourceElementLabel.equals(target.sourceElementLabel)
                    && destElementLabel.equals(target.destElementLabel) && edgeLabel.equals(target.edgeLabel);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceElementLabel, destElementLabel, edgeLabel);
        }
    }
}
