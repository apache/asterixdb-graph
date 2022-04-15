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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Metadata representation of an edge. An edge consists of the following:
 * 1. A {@link GraphElementIdentifier}, to uniquely identify the edge across other graph elements.
 * 2. A source vertex ({@link Vertex}) reference.
 * 3. A destination vertex ({@link Vertex}) reference.
 * 4. A collection of edge definitions, which contains a source key, destination key, and a SQL++ string.
 */
public class Edge implements Element {
    private static final long serialVersionUID = 1L;

    private final GraphElementIdentifier identifier;
    private final Vertex destinationVertex;
    private final Vertex sourceVertex;
    private final List<Definition> definitions;

    /**
     * Use {@link Schema.Builder} to build Edge instances instead of this constructor.
     */
    Edge(GraphElementIdentifier identifier, Vertex destinationVertex, Vertex sourceVertex, Definition edgeDefinition) {
        this.destinationVertex = Objects.requireNonNull(destinationVertex);
        this.sourceVertex = Objects.requireNonNull(sourceVertex);
        this.identifier = Objects.requireNonNull(identifier);
        this.definitions = new ArrayList<>();
        this.definitions.add(Objects.requireNonNull(edgeDefinition));
    }

    public static class Definition {
        private final List<List<String>> destinationKeyFieldNames;
        private final List<List<String>> sourceKeyFieldNames;
        private final String definition;

        Definition(List<List<String>> destinationKeyFieldNames, List<List<String>> sourceKeyFieldNames,
                String definition) {
            this.destinationKeyFieldNames = Objects.requireNonNull(destinationKeyFieldNames);
            this.sourceKeyFieldNames = Objects.requireNonNull(sourceKeyFieldNames);
            this.definition = Objects.requireNonNull(definition);
        }

        public List<List<String>> getDestinationKeyFieldNames() {
            return destinationKeyFieldNames;
        }

        public List<List<String>> getSourceKeyFieldNames() {
            return sourceKeyFieldNames;
        }

        public String getDefinition() {
            return definition;
        }
    }

    public ElementLabel getDestinationLabel() {
        return destinationVertex.getLabel();
    }

    public ElementLabel getSourceLabel() {
        return sourceVertex.getLabel();
    }

    public Vertex getDestinationVertex() {
        return destinationVertex;
    }

    public Vertex getSourceVertex() {
        return sourceVertex;
    }

    public List<Definition> getDefinitions() {
        return definitions;
    }

    @Override
    public GraphElementIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public ElementLabel getLabel() {
        return identifier.getElementLabel();
    }

    @Override
    public List<String> getDefinitionBodies() {
        return definitions.stream().map(Definition::getDefinition).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        String edgeBodyPattern = "[:" + getLabel() + "]";
        String sourceNodePattern = "(:" + getSourceLabel() + ")";
        String destinationNodePattern = "(:" + getDestinationLabel() + ")";
        String edgePattern = sourceNodePattern + "-" + edgeBodyPattern + "->" + destinationNodePattern;
        return edgePattern + " AS " + String.join(",\n", getDefinitionBodies());
    }
}
