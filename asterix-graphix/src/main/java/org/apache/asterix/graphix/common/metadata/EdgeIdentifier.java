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
package org.apache.asterix.graphix.common.metadata;

import java.util.Objects;

import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * A unique identifier for an edge. An edge is uniquely identified by:
 * <ul>
 *  <li>The graph identifier associated with the edge itself.</li>
 *  <li>The label associated with the source vertex.</li>
 *  <li>The label associated with the destination vertex.</li>
 *  <li>The label associated with the edge itself.</li>
 * </ul>
 */
public class EdgeIdentifier implements IElementIdentifier {
    private static final long serialVersionUID = 1L;
    private final GraphIdentifier graphIdentifier;
    private final ElementLabel sourceLabel;
    private final ElementLabel edgeLabel;
    private final ElementLabel destinationLabel;

    public EdgeIdentifier(GraphIdentifier graphIdentifier, ElementLabel sourceLabel, ElementLabel edgeLabel,
            ElementLabel destinationLabel) {
        this.graphIdentifier = Objects.requireNonNull(graphIdentifier);
        this.sourceLabel = Objects.requireNonNull(sourceLabel);
        this.edgeLabel = Objects.requireNonNull(edgeLabel);
        this.destinationLabel = Objects.requireNonNull(destinationLabel);
    }

    @Override
    public GraphIdentifier getGraphIdentifier() {
        return graphIdentifier;
    }

    public ElementLabel getSourceLabel() {
        return sourceLabel;
    }

    public ElementLabel getEdgeLabel() {
        return edgeLabel;
    }

    public ElementLabel getDestinationLabel() {
        return destinationLabel;
    }

    @Override
    public String toString() {
        return String.format("%s (:%s)-[:%s]->(:%s)", graphIdentifier, sourceLabel, edgeLabel, destinationLabel);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof EdgeIdentifier) {
            EdgeIdentifier that = (EdgeIdentifier) o;
            return Objects.equals(this.graphIdentifier, that.graphIdentifier)
                    && Objects.equals(this.sourceLabel, that.sourceLabel)
                    && Objects.equals(this.edgeLabel, that.edgeLabel)
                    && Objects.equals(this.destinationLabel, that.destinationLabel);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphIdentifier, sourceLabel, edgeLabel, destinationLabel);
    }
}
