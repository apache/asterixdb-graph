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
 * Metadata representation of a vertex. A vertex consists of the following:
 * 1. A {@link GraphElementIdentifier}, to uniquely identify the vertex across other graph elements.
 * 2. A collection of vertex definitions, each of which consists of a primary key and a SQL++ string.
 */
public class Vertex implements Element {
    private static final long serialVersionUID = 1L;

    private final GraphElementIdentifier identifier;
    private final List<Definition> definitions;

    /**
     * Use {@link Schema.Builder} to build Vertex instances instead of this constructor.
     */
    Vertex(GraphElementIdentifier identifier, Definition definition) {
        this.identifier = Objects.requireNonNull(identifier);
        this.definitions = new ArrayList<>();
        this.definitions.add(Objects.requireNonNull(definition));
    }

    public static class Definition {
        private final List<List<String>> primaryKeyFieldNames;
        private final String definition;

        Definition(List<List<String>> primaryKeyFieldNames, String definition) {
            this.primaryKeyFieldNames = primaryKeyFieldNames;
            this.definition = definition;
        }

        public List<List<String>> getPrimaryKeyFieldNames() {
            return primaryKeyFieldNames;
        }

        public String getDefinition() {
            return definition;
        }

        @Override
        public String toString() {
            return definition;
        }
    }

    // A primary key is the same across all vertex definitions.
    public List<List<String>> getPrimaryKeyFieldNames() {
        return definitions.get(0).getPrimaryKeyFieldNames();
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
        return "(:" + getLabel() + ") AS " + String.join(",\n", getDefinitionBodies());
    }
}
