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
import java.util.List;
import java.util.Map;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Metadata representation of a graph schema. A graph schema consists of:
 * 1. A list of {@link Vertex} instances.
 * 2. A list of {@link Edge} instances, which link the aforementioned vertices.
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
        private final Map<ElementLabel, List<Edge>> edgeLabelMap = new HashMap<>();

        // We aim to populate the schema object below.
        private final Schema workingSchema;
        private final GraphIdentifier graphIdentifier;
        private Error lastError = Error.NO_ERROR;

        public Builder(GraphIdentifier graphIdentifier) {
            this.graphIdentifier = graphIdentifier;
            this.workingSchema = new Schema();
        }

        /**
         * @return Null if the primary keys of an existing vertex conflict with the vertex to-be-added. The vertex
         * to-be-added otherwise.
         */
        public Vertex addVertex(ElementLabel vertexLabel, List<List<String>> primaryKeyFieldNames, String definition) {
            if (!vertexLabelMap.containsKey(vertexLabel)) {
                GraphElementIdentifier identifier =
                        new GraphElementIdentifier(graphIdentifier, GraphElementIdentifier.Kind.VERTEX, vertexLabel);
                Vertex newVertex = new Vertex(identifier, new Vertex.Definition(primaryKeyFieldNames, definition));
                workingSchema.vertexList.add(newVertex);
                vertexLabelMap.put(vertexLabel, newVertex);
                return newVertex;

            } else {
                Vertex existingVertex = vertexLabelMap.get(vertexLabel);
                if (!existingVertex.getPrimaryKeyFieldNames().equals(primaryKeyFieldNames)) {
                    lastError = Error.CONFLICTING_PRIMARY_KEY;
                    return null;
                }
                existingVertex.getDefinitions().add(new Vertex.Definition(primaryKeyFieldNames, definition));
                return existingVertex;
            }
        }

        /**
         * @return Null if there exists no vertex with the given source label or destination label, OR if the
         * source vertex / destination vertex of an existing edge conflict with the edge to-be-added, OR if the source
         * key / destination key of an existing edge conflict with the edge to-be-added. Otherwise, the edge
         * to-be-added.
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

            if (edgeLabelMap.containsKey(edgeLabel)) {
                for (Edge existingEdge : edgeLabelMap.get(edgeLabel)) {
                    ElementLabel existingSourceLabel = existingEdge.getSourceLabel();
                    ElementLabel existingDestLabel = existingEdge.getDestinationLabel();
                    if (existingSourceLabel.equals(sourceLabel) && existingDestLabel.equals(destinationLabel)) {
                        Edge.Definition existingDefinition = existingEdge.getDefinitions().get(0);
                        if (!existingDefinition.getSourceKeyFieldNames().equals(sourceKeyFieldNames)) {
                            lastError = Error.CONFLICTING_SOURCE_KEY;
                            return null;

                        } else if (!existingDefinition.getDestinationKeyFieldNames().equals(destinationKeyFieldNames)) {
                            lastError = Error.CONFLICTING_DESTINATION_KEY;
                            return null;

                        } else {
                            Edge.Definition newEdgeDefinition =
                                    new Edge.Definition(destinationKeyFieldNames, sourceKeyFieldNames, definitionBody);
                            existingEdge.getDefinitions().add(newEdgeDefinition);
                            return existingEdge;
                        }
                    }
                }
            }

            // Update our schema.
            GraphElementIdentifier identifier =
                    new GraphElementIdentifier(graphIdentifier, GraphElementIdentifier.Kind.EDGE, edgeLabel);
            Edge newEdge = new Edge(identifier, vertexLabelMap.get(destinationLabel), vertexLabelMap.get(sourceLabel),
                    new Edge.Definition(destinationKeyFieldNames, sourceKeyFieldNames, definitionBody));
            workingSchema.edgeList.add(newEdge);

            // Update our edge label map.
            ArrayList<Edge> edgeList = new ArrayList<>();
            edgeList.add(newEdge);
            edgeLabelMap.put(edgeLabel, edgeList);
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

            CONFLICTING_PRIMARY_KEY,
            CONFLICTING_SOURCE_KEY,
            CONFLICTING_DESTINATION_KEY,

            SOURCE_VERTEX_NOT_FOUND,
            DESTINATION_VERTEX_NOT_FOUND
        }
    }
}
