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

import java.util.List;
import java.util.Objects;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Metadata representation of an edge. An edge consists of the following:
 * 1. A {@link GraphElementIdentifier}, to uniquely identify the edge across other graph elements.
 * 2. An {@link ElementLabel} instance associated with the source vertex.
 * 3. An {@link ElementLabel} instance associated with the destination vertex.
 * 4. A list of source key fields, associated with the definition body.
 * 5. A list of destination key fields, associated with the definition body.
 * 6. A SQL++ string denoting the definition body.
 */
public class Edge implements IElement {
    private static final long serialVersionUID = 1L;

    private final GraphElementIdentifier identifier;
    private final ElementLabel sourceVertexLabel;
    private final ElementLabel destinationVertexLabel;
    private final List<List<String>> sourceKeyFieldNames;
    private final List<List<String>> destinationKeyFieldNames;
    private final String definitionBody;

    /**
     * Use {@link Schema.Builder} to build Edge instances instead of this constructor.
     */
    Edge(GraphElementIdentifier identifier, ElementLabel sourceVertexLabel, ElementLabel destinationVertexLabel,
            List<List<String>> sourceKeyFieldNames, List<List<String>> destKeyFieldNames, String definitionBody) {
        this.identifier = Objects.requireNonNull(identifier);
        this.sourceVertexLabel = Objects.requireNonNull(sourceVertexLabel);
        this.destinationVertexLabel = Objects.requireNonNull(destinationVertexLabel);
        this.sourceKeyFieldNames = Objects.requireNonNull(sourceKeyFieldNames);
        this.destinationKeyFieldNames = Objects.requireNonNull(destKeyFieldNames);
        this.definitionBody = Objects.requireNonNull(definitionBody);
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
        return destinationVertexLabel;
    }

    public ElementLabel getSourceLabel() {
        return sourceVertexLabel;
    }

    public List<List<String>> getSourceKeyFieldNames() {
        return sourceKeyFieldNames;
    }

    public List<List<String>> getDestinationKeyFieldNames() {
        return destinationKeyFieldNames;
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
    public String getDefinitionBody() {
        return definitionBody;
    }

    @Override
    public String toString() {
        String edgeBodyPattern = "[:" + getLabel() + "]";
        String sourceNodePattern = "(:" + getSourceLabel() + ")";
        String destinationNodePattern = "(:" + getDestinationLabel() + ")";
        String edgePattern = sourceNodePattern + "-" + edgeBodyPattern + "->" + destinationNodePattern;
        return edgePattern + " AS " + definitionBody;
    }
}
