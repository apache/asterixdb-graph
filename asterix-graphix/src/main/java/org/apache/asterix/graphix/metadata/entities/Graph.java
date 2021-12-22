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
package org.apache.asterix.graphix.metadata.entities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.metadata.bootstrap.GraphixMetadataIndexes;
import org.apache.asterix.metadata.api.ExtensionMetadataDatasetId;
import org.apache.asterix.metadata.api.IExtensionMetadataEntity;

/**
 * Metadata describing a graph view, composed of vertices and edges.
 */
public class Graph implements IExtensionMetadataEntity {
    private static final long serialVersionUID = 1L;

    private final GraphIdentifier identifier;
    private final GraphDependencies dependencies;
    private final Schema graphSchema;

    public Graph(GraphIdentifier identifier, Schema graphSchema, GraphDependencies dependencies) {
        this.identifier = Objects.requireNonNull(identifier);
        this.dependencies = dependencies;
        this.graphSchema = graphSchema;
    }

    public DataverseName getDataverseName() {
        return identifier.getDataverseName();
    }

    public String getGraphName() {
        return identifier.getGraphName();
    }

    public GraphIdentifier getIdentifier() {
        return identifier;
    }

    public Schema getGraphSchema() {
        return graphSchema;
    }

    public GraphDependencies getDependencies() {
        return dependencies;
    }

    @Override
    public ExtensionMetadataDatasetId getDatasetId() {
        return GraphixMetadataIndexes.GRAPH_METADATA_DATASET_EXTENSION_ID;
    }

    public static class Schema implements Serializable {
        private static final long serialVersionUID = 1L;

        // The element map is composed of the vertices and edges.
        private final Map<GraphElementIdentifier, Element> elementMap = new HashMap<>();
        private final List<Vertex> vertexList = new ArrayList<>();
        private final List<Edge> edgeList = new ArrayList<>();

        public List<Vertex> getVertices() {
            return vertexList;
        }

        public List<Edge> getEdges() {
            return edgeList;
        }

        public Element getElement(GraphElementIdentifier identifier) {
            return elementMap.get(identifier);
        }

        private Schema() {
        }

        public static class Builder {
            private final Map<String, Vertex> vertexLabelMap = new HashMap<>();
            private final Map<String, Edge> edgeLabelMap = new HashMap<>();

            // We aim to populate the schema object below.
            private final Schema workingSchema = new Schema();
            private final GraphIdentifier graphIdentifier;
            private Error lastError = Error.NO_ERROR;

            public Builder(GraphIdentifier graphIdentifier) {
                this.graphIdentifier = graphIdentifier;
            }

            /**
             * @return Null if the primary keys of an existing vertex conflict with the vertex to-be-added. The vertex
             * to-be-added otherwise.
             */
            public Vertex addVertex(String labelName, List<List<String>> primaryKeyFieldNames, String definition) {
                if (!vertexLabelMap.containsKey(labelName)) {
                    GraphElementIdentifier identifier =
                            new GraphElementIdentifier(graphIdentifier, GraphElementIdentifier.Kind.VERTEX, labelName);
                    Vertex newVertex = new Vertex(identifier, primaryKeyFieldNames, definition);
                    workingSchema.vertexList.add(newVertex);
                    vertexLabelMap.put(labelName, newVertex);
                    return newVertex;

                } else {
                    Vertex existingVertex = vertexLabelMap.get(labelName);
                    if (!existingVertex.getPrimaryKeyFieldNames().equals(primaryKeyFieldNames)) {
                        lastError = Error.CONFLICTING_PRIMARY_KEY;
                        return null;
                    }
                    existingVertex.getDefinitions().add(definition);
                    return existingVertex;
                }
            }

            /**
             * @return Null if there exists no vertex with the given source label or destination label, OR if the
             * primary key / source vertex / destination vertex of an existing edge conflict with the edge to-be-added.
             */
            public Edge addEdge(String edgeLabelName, String destinationLabelName, String sourceLabelName,
                    List<List<String>> destinationKeyFieldNames) {
                if (!vertexLabelMap.containsKey(sourceLabelName)) {
                    lastError = Error.SOURCE_VERTEX_NOT_FOUND;
                    return null;
                }

                Vertex representativeSourceVertex = vertexLabelMap.get(sourceLabelName);
                return addEdge(edgeLabelName, destinationLabelName, sourceLabelName,
                        representativeSourceVertex.getPrimaryKeyFieldNames(), destinationKeyFieldNames,
                        representativeSourceVertex.getPrimaryKeyFieldNames(), "");
            }

            /**
             * @return Null if there exists no vertex with the given source label or destination label, OR if the
             * primary key / source vertex / destination vertex of an existing edge conflict with the edge to-be-added.
             */
            public Edge addEdge(String edgeLabelName, String destinationLabelName, String sourceLabelName,
                    List<List<String>> primaryKeyFieldNames, List<List<String>> destinationKeyFieldNames,
                    List<List<String>> sourceKeyFieldNames, String definition) {
                if (!vertexLabelMap.containsKey(sourceLabelName)) {
                    lastError = Error.SOURCE_VERTEX_NOT_FOUND;
                    return null;

                } else if (!vertexLabelMap.containsKey(destinationLabelName)) {
                    lastError = Error.DESTINATION_VERTEX_NOT_FOUND;
                    return null;

                } else if (edgeLabelMap.containsKey(edgeLabelName)) {
                    Edge existingEdge = edgeLabelMap.get(edgeLabelName);
                    if (!existingEdge.getPrimaryKeyFieldNames().equals(primaryKeyFieldNames)) {
                        lastError = Error.CONFLICTING_PRIMARY_KEY;
                        return null;

                    } else if (!existingEdge.getSourceLabelName().equals(sourceLabelName)) {
                        // This also covers any source-key conflicts.
                        lastError = Error.CONFLICTING_SOURCE_VERTEX;
                        return null;

                    } else if (!existingEdge.getDestinationLabelName().equals(destinationLabelName)) {
                        // This also covers any destination-key conflicts.
                        lastError = Error.CONFLICTING_DESTINATION_VERTEX;
                        return null;
                    }
                    existingEdge.getDefinitions().add(definition);
                    return existingEdge;

                } else {
                    GraphElementIdentifier identifier = new GraphElementIdentifier(graphIdentifier,
                            GraphElementIdentifier.Kind.EDGE, edgeLabelName);
                    Edge newEdge = new Edge(identifier, primaryKeyFieldNames, destinationKeyFieldNames,
                            sourceKeyFieldNames, vertexLabelMap.get(destinationLabelName),
                            vertexLabelMap.get(sourceLabelName), definition);
                    workingSchema.edgeList.add(newEdge);
                    edgeLabelMap.put(edgeLabelName, newEdge);
                    return newEdge;
                }
            }

            public Schema build() {
                // Build the element map, composed of our vertices and edges.
                workingSchema.elementMap.clear();
                workingSchema.getVertices().forEach(v -> workingSchema.elementMap.put(v.identifier, v));
                workingSchema.getEdges().forEach(e -> workingSchema.elementMap.put(e.identifier, e));
                return workingSchema;
            }

            public Error getLastError() {
                return lastError;
            }

            public enum Error {
                NO_ERROR,

                CONFLICTING_PRIMARY_KEY,
                CONFLICTING_SOURCE_VERTEX,
                CONFLICTING_DESTINATION_VERTEX,

                SOURCE_VERTEX_NOT_FOUND,
                DESTINATION_VERTEX_NOT_FOUND
            }
        }
    }

    public static final class Vertex implements Element {
        private static final long serialVersionUID = 1L;

        private final GraphElementIdentifier identifier;
        private final List<List<String>> primaryKeyFieldNames;
        private final List<String> definitions;

        private Vertex(GraphElementIdentifier identifier, List<List<String>> primaryKeyFieldNames, String definition) {
            this.identifier = Objects.requireNonNull(identifier);
            this.primaryKeyFieldNames = Objects.requireNonNull(primaryKeyFieldNames);
            this.definitions = new ArrayList<>();
            this.definitions.add(Objects.requireNonNull(definition));
        }

        public List<List<String>> getPrimaryKeyFieldNames() {
            return primaryKeyFieldNames;
        }

        @Override
        public GraphElementIdentifier getIdentifier() {
            return identifier;
        }

        @Override
        public String getLabelName() {
            return identifier.getLabelName();
        }

        @Override
        public List<String> getDefinitions() {
            return definitions;
        }

        @Override
        public String toString() {
            return "(:" + getLabelName() + ") AS " + String.join(",\n", definitions);
        }
    }

    public static final class Edge implements Element {
        private static final long serialVersionUID = 1L;

        private final List<List<String>> primaryKeyFieldNames;
        private final List<List<String>> destinationKeyFieldNames;
        private final List<List<String>> sourceKeyFieldNames;

        private final GraphElementIdentifier identifier;
        private final Vertex destinationVertex;
        private final Vertex sourceVertex;
        private final List<String> definitions;

        private Edge(GraphElementIdentifier identifier, List<List<String>> primaryKeyFieldNames,
                List<List<String>> destinationKeyFieldNames, List<List<String>> sourceKeyFieldNames,
                Vertex destinationVertex, Vertex sourceVertex, String edgeDefinition) {
            this.primaryKeyFieldNames = Objects.requireNonNull(primaryKeyFieldNames);
            this.destinationKeyFieldNames = Objects.requireNonNull(destinationKeyFieldNames);
            this.sourceKeyFieldNames = Objects.requireNonNull(sourceKeyFieldNames);
            this.destinationVertex = Objects.requireNonNull(destinationVertex);
            this.sourceVertex = Objects.requireNonNull(sourceVertex);
            this.identifier = Objects.requireNonNull(identifier);
            this.definitions = new ArrayList<>();
            this.definitions.add(Objects.requireNonNull(edgeDefinition));
        }

        public String getDestinationLabelName() {
            return destinationVertex.getLabelName();
        }

        public String getSourceLabelName() {
            return sourceVertex.getLabelName();
        }

        public List<List<String>> getPrimaryKeyFieldNames() {
            return primaryKeyFieldNames;
        }

        public List<List<String>> getDestinationKeyFieldNames() {
            return destinationKeyFieldNames;
        }

        public List<List<String>> getSourceKeyFieldNames() {
            return sourceKeyFieldNames;
        }

        public Vertex getDestinationVertex() {
            return destinationVertex;
        }

        public Vertex getSourceVertex() {
            return sourceVertex;
        }

        @Override
        public GraphElementIdentifier getIdentifier() {
            return identifier;
        }

        @Override
        public String getLabelName() {
            return identifier.getLabelName();
        }

        @Override
        public List<String> getDefinitions() {
            return definitions;
        }

        @Override
        public String toString() {
            String edgeBodyPattern = "[:" + getLabelName() + "]";
            String sourceNodePattern = "(:" + getSourceLabelName() + ")";
            String destinationNodePattern = "(:" + getDestinationLabelName() + ")";
            String edgePattern = sourceNodePattern + "-" + edgeBodyPattern + "->" + destinationNodePattern;
            return edgePattern + " AS " + String.join(",\n", getDefinitions());
        }
    }

    public interface Element extends Serializable {
        GraphElementIdentifier getIdentifier();

        String getLabelName();

        List<String> getDefinitions();
    }
}
