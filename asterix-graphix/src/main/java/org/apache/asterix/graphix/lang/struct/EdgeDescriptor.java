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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.expression.IGraphExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Descriptor for a query edge instance. A query edge has the following:
 * 1. A set of edge labels.
 * 2. A variable associated with the query edge.
 * 3. An edge class. An edge can either be a pure edge, or a sub-path.
 * 4. A minimum number of hops (allowed to be NULL, indicating a minimum of 1 hop).
 * 5. A maximum number of hops (not allowed to be NULL).
 * 6. An edge type, which denotes the direction (left to right, right to left, or undirected).
 */
public class EdgeDescriptor {
    private final IGraphExpr.GraphExprKind edgeClass;
    private final Set<ElementLabel> edgeLabels;
    private final Integer minimumHops;
    private final Integer maximumHops;

    // We must be able to assign variables to our edges, as well as change the direction of UNDIRECTED edges.
    private VariableExpr variableExpr;
    private EdgeType edgeType;

    public EdgeDescriptor(EdgeType edgeType, IGraphExpr.GraphExprKind edgeClass, Set<ElementLabel> edgeLabels,
            VariableExpr variableExpr, Integer minimumHops, Integer maximumHops) {
        this.edgeType = edgeType;
        this.edgeLabels = edgeLabels;
        this.minimumHops = minimumHops;
        this.maximumHops = maximumHops;
        this.edgeClass = edgeClass;
        this.variableExpr = variableExpr;

        // We enforce that an edge can take two forms: as a sub-path, and as a pure "edge".
        switch (edgeClass) {
            case GRAPH_CONSTRUCTOR:
            case GRAPH_ELEMENT_BODY:
            case VERTEX_PATTERN:
                throw new IllegalArgumentException(
                        "Illegal class of edge specified! An edge can only be a PATH or an EDGE.");
        }
    }

    public EdgeType getEdgeType() {
        return edgeType;
    }

    public void setEdgeType(EdgeType edgeType) {
        this.edgeType = edgeType;
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

    public IGraphExpr.GraphExprKind getEdgeClass() {
        return edgeClass;
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
    public String toString() {
        String labelsString = edgeLabels.stream().map(ElementLabel::toString).collect(Collectors.joining("|"));
        String variableString = (variableExpr != null) ? variableExpr.getVar().toString() : "";
        String subPathString = (edgeClass != IGraphExpr.GraphExprKind.PATH_PATTERN) ? ""
                : "{" + ((minimumHops == null) ? "" : minimumHops) + "," + maximumHops + "}";
        return String.format("%s-[%s:(%s)%s]-%s", (edgeType == EdgeType.LEFT_TO_RIGHT) ? "" : "<", variableString,
                labelsString, subPathString, (edgeType == EdgeType.RIGHT_TO_LEFT) ? "" : ">");
    }

    public enum EdgeType {
        LEFT_TO_RIGHT,
        RIGHT_TO_LEFT,
        UNDIRECTED
    }
}
