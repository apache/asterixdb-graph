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
package org.apache.asterix.graphix.lang.rewrites.resolve;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.metadata.entity.schema.Graph;
import org.apache.asterix.graphix.metadata.entity.schema.Vertex;

/**
 * A collection of ground truths, derived from the graph schema (either a {@link Graph} or {@link GraphConstructor}).
 *
 * @see InferenceBasedResolver
 */
public class SchemaKnowledgeTable implements Iterable<SchemaKnowledgeTable.KnowledgeRecord> {
    private final Set<KnowledgeRecord> knowledgeRecordSet = new HashSet<>();
    private final Set<ElementLabel> vertexLabelSet = new HashSet<>();
    private final Set<ElementLabel> edgeLabelSet = new HashSet<>();

    public SchemaKnowledgeTable(Graph graph) {
        graph.getGraphSchema().getVertices().stream().map(Vertex::getLabel).forEach(vertexLabelSet::add);
        graph.getGraphSchema().getEdges().forEach(e -> {
            vertexLabelSet.add(e.getSourceLabel());
            vertexLabelSet.add(e.getDestinationLabel());
            edgeLabelSet.add(e.getLabel());
            knowledgeRecordSet.add(new KnowledgeRecord(e.getSourceLabel(), e.getDestinationLabel(), e.getLabel()));
        });
    }

    public SchemaKnowledgeTable(GraphConstructor graphConstructor) {
        graphConstructor.getVertexElements().forEach(v -> vertexLabelSet.add(v.getLabel()));
        graphConstructor.getEdgeElements().forEach(e -> {
            vertexLabelSet.add(e.getSourceLabel());
            vertexLabelSet.add(e.getDestinationLabel());
            edgeLabelSet.add(e.getEdgeLabel());
            knowledgeRecordSet.add(new KnowledgeRecord(e.getSourceLabel(), e.getDestinationLabel(), e.getEdgeLabel()));
        });
    }

    public Set<ElementLabel> getVertexLabelSet() {
        return vertexLabelSet;
    }

    public Set<ElementLabel> getEdgeLabelSet() {
        return edgeLabelSet;
    }

    @Override
    public Iterator<KnowledgeRecord> iterator() {
        return knowledgeRecordSet.iterator();
    }

    public static class KnowledgeRecord {
        private final ElementLabel sourceVertexLabel;
        private final ElementLabel destVertexLabel;
        private final ElementLabel edgeLabel;

        public KnowledgeRecord(ElementLabel sourceVertexLabel, ElementLabel destVertexLabel, ElementLabel edgeLabel) {
            this.sourceVertexLabel = Objects.requireNonNull(sourceVertexLabel);
            this.destVertexLabel = Objects.requireNonNull(destVertexLabel);
            this.edgeLabel = Objects.requireNonNull(edgeLabel);
        }

        public ElementLabel getSourceVertexLabel() {
            return sourceVertexLabel;
        }

        public ElementLabel getDestVertexLabel() {
            return destVertexLabel;
        }

        public ElementLabel getEdgeLabel() {
            return edgeLabel;
        }

        @Override
        public String toString() {
            return String.format("(%s)-[%s]->(%s)", sourceVertexLabel, edgeLabel, destVertexLabel);
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
            return sourceVertexLabel.equals(target.sourceVertexLabel) && destVertexLabel.equals(target.destVertexLabel)
                    && edgeLabel.equals(target.edgeLabel);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceVertexLabel, destVertexLabel, edgeLabel);
        }
    }
}
