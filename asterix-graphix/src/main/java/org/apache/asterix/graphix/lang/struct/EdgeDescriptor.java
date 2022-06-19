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
package org.apache.asterix.graphix.lang.struct;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Descriptor for a query edge instance. A query edge has the following:
 * 1. A set of edge labels.
 * 2. A variable associated with the query edge.
 * 3. A pattern type. An edge pattern can either be a pure edge, or a sub-path.
 * 4. A minimum number of hops (allowed to be NULL, indicating a minimum of 1 hop).
 * 5. A maximum number of hops (not allowed to be NULL).
 * 6. An edge direction (left to right, right to left, or undirected).
 */
public class EdgeDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Set<ElementLabel> edgeLabels;
    private final Integer minimumHops;
    private final Integer maximumHops;
    private final PatternType patternType;

    // We must be able to assign variables to our edges, as well as change the direction of UNDIRECTED edges.
    private VariableExpr variableExpr;
    private EdgeDirection edgeDirection;

    public EdgeDescriptor(EdgeDirection edgeDirection, PatternType patternType, Set<ElementLabel> edgeLabels,
            VariableExpr variableExpr, Integer minimumHops, Integer maximumHops) {
        this.edgeDirection = edgeDirection;
        this.edgeLabels = edgeLabels;
        this.minimumHops = minimumHops;
        this.maximumHops = maximumHops;
        this.patternType = patternType;
        this.variableExpr = variableExpr;
    }

    public EdgeDirection getEdgeDirection() {
        return edgeDirection;
    }

    public void setEdgeDirection(EdgeDirection edgeDirection) {
        this.edgeDirection = edgeDirection;
    }

    public Set<ElementLabel> getEdgeLabels() {
        return edgeLabels;
    }

    public Integer getMinimumHops() {
        return minimumHops;
    }

    public Integer getMaximumHops() {
        return maximumHops;
    }

    public PatternType getPatternType() {
        return patternType;
    }

    public VariableExpr getVariableExpr() {
        return variableExpr;
    }

    public void setVariableExpr(VariableExpr variableExpr) {
        this.variableExpr = variableExpr;
    }

    public List<GraphElementIdentifier> generateIdentifiers(GraphIdentifier graphIdentifier) {
        return edgeLabels.stream()
                .map(e -> new GraphElementIdentifier(graphIdentifier, GraphElementIdentifier.Kind.EDGE, e))
                .collect(Collectors.toList());
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeDirection, patternType, edgeLabels, variableExpr, minimumHops, maximumHops);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof EdgeDescriptor)) {
            return false;
        }
        EdgeDescriptor that = (EdgeDescriptor) object;
        return Objects.equals(this.edgeDirection, that.edgeDirection)
                && Objects.equals(this.patternType, that.patternType)
                && Objects.equals(this.edgeLabels, that.edgeLabels)
                && Objects.equals(this.variableExpr, that.variableExpr)
                && Objects.equals(this.minimumHops, that.minimumHops)
                && Objects.equals(this.maximumHops, that.maximumHops);
    }

    @Override
    public String toString() {
        String labelsString = edgeLabels.stream().map(ElementLabel::toString).collect(Collectors.joining("|"));
        String variableString = (variableExpr != null) ? variableExpr.getVar().toString() : "";
        String subPathString = (patternType != PatternType.PATH) ? ""
                : "{" + ((minimumHops == null) ? "" : minimumHops) + "," + maximumHops + "}";
        return String.format("%s-[%s:(%s)%s]-%s", (edgeDirection == EdgeDirection.LEFT_TO_RIGHT) ? "" : "<",
                variableString, labelsString, subPathString, (edgeDirection == EdgeDirection.RIGHT_TO_LEFT) ? "" : ">");
    }

    public enum EdgeDirection {
        LEFT_TO_RIGHT,
        RIGHT_TO_LEFT,
        UNDIRECTED
    }

    public enum PatternType {
        PATH,
        EDGE
    }
}
