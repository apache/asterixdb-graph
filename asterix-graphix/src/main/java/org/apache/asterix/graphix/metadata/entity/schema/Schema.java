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
package org.apache.asterix.graphix.metadata.entity.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.common.metadata.VertexIdentifier;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Metadata representation of a graph schema. A graph schema consists of:
 * <ul>
 *  <li>A list of {@link Vertex} instances.</li>
 *  <li>A list of {@link Edge} instances, which link the aforementioned vertices.</li>
 * </ul>
 */
public class Schema implements Serializable {
    private static final long serialVersionUID = 1L;

    // The element map is composed of the vertices and edges.
    private final List<Vertex> vertexList = new ArrayList<>();
    private final List<Edge> edgeList = new ArrayList<>();

    public List<Vertex> getVertices() {
        return vertexList;
    }

    public List<Edge> getEdges() {
        return edgeList;
    }

    /**
     * Use the {@link Builder} class to create Schema instances.
     */
    private Schema() {
    }

    public static class Builder {
        private final Map<ElementLabel, Vertex> vertexLabelMap = new HashMap<>();
        private final Map<EdgeLabel, List<Edge>> edgeLabelMap = new HashMap<>();

        // We aim to populate the schema object below.
        private final Schema workingSchema;
        private final GraphIdentifier graphIdentifier;
        private Error lastError = Error.NO_ERROR;

        public Builder(GraphIdentifier graphIdentifier) {
            this.graphIdentifier = graphIdentifier;
            this.workingSchema = new Schema();
        }

        /**
         * @return Null if a vertex with the same label already exists. The vertex to-be-added otherwise.
         */
        public Vertex addVertex(ElementLabel elementLabel, List<List<String>> primaryKeyFieldNames, String definition) {
            if (!vertexLabelMap.containsKey(elementLabel)) {
                VertexIdentifier identifier = new VertexIdentifier(graphIdentifier, elementLabel);
                Vertex newVertex = new Vertex(identifier, primaryKeyFieldNames, definition);
                workingSchema.vertexList.add(newVertex);
                vertexLabelMap.put(elementLabel, newVertex);
                return newVertex;

            } else {
                lastError = Error.VERTEX_LABEL_CONFLICT;
                return null;
            }
        }

        /**
         * @return Null if there exists no vertex with the given source label or destination label, OR if an edge with
         * the same label already exists. The edge to-be-added otherwise.
         */
        public Edge addEdge(ElementLabel edgeLabel, ElementLabel destinationLabel, ElementLabel sourceLabel,
                List<List<String>> destinationKeyFieldNames, List<List<String>> sourceKeyFieldNames,
                String definitionBody) {
            if (!vertexLabelMap.containsKey(sourceLabel)) {
                lastError = Error.SOURCE_VERTEX_NOT_FOUND;
                return null;

            } else if (!vertexLabelMap.containsKey(destinationLabel)) {
                lastError = Error.DESTINATION_VERTEX_NOT_FOUND;
                return null;
            }

            // Ensure we have unique <source, edge, dest> triples.
            EdgeLabel edgePatternLabel = new EdgeLabel();
            edgePatternLabel.endpointLabels.add(sourceLabel);
            edgePatternLabel.endpointLabels.add(destinationLabel);
            edgePatternLabel.edgeLabel = edgeLabel;
            if (edgeLabelMap.containsKey(edgePatternLabel)) {
                lastError = Error.EDGE_LABEL_CONFLICT;
                return null;
            }

            // Update our schema.
            EdgeIdentifier identifier = new EdgeIdentifier(graphIdentifier, sourceLabel, edgeLabel, destinationLabel);
            Edge newEdge = new Edge(identifier, sourceKeyFieldNames, destinationKeyFieldNames, definitionBody);
            workingSchema.edgeList.add(newEdge);

            // Update our edge label map.
            ArrayList<Edge> edgeList = new ArrayList<>();
            edgeList.add(newEdge);
            edgeLabelMap.put(edgePatternLabel, edgeList);
            return newEdge;
        }

        public Schema build() {
            return workingSchema;
        }

        public Error getLastError() {
            return lastError;
        }

        public enum Error {
            NO_ERROR,
            VERTEX_LABEL_CONFLICT,
            EDGE_LABEL_CONFLICT,
            SOURCE_VERTEX_NOT_FOUND,
            DESTINATION_VERTEX_NOT_FOUND
        }
    }

    private static class EdgeLabel {
        public Set<ElementLabel> endpointLabels = new HashSet<>();
        public ElementLabel edgeLabel = null;

        @Override
        public int hashCode() {
            return Objects.hash(edgeLabel, endpointLabels);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof EdgeLabel) {
                EdgeLabel that = (EdgeLabel) o;
                return Objects.equals(this.endpointLabels, that.endpointLabels)
                        && Objects.equals(this.edgeLabel, that.edgeLabel);
            }
            return false;
        }
    }
}
