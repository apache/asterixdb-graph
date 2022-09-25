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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;

/**
 * Descriptor for a query edge instance. A query edge has the following:
 * <ul>
 *  <li>A set of edge labels.</li>
 *  <li>A variable associated with the query edge.</li>
 *  <li>A pattern type. An edge pattern can either be a pure edge, or a sub-path.</li>
 *  <li>A minimum number of hops (allowed to be NULL, indicating a minimum of 1 hop).</li>
 *  <li>A maximum number of hops (allowed to be NULL, indicating an unbounded maximum).</li>
 *  <li>An edge direction (left to right, right to left, or undirected).</li>
 *  <li>A filter expression (allowed to be NULL).</li>
 * </ul>
 */
public class EdgeDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Set<ElementLabel> edgeLabels;
    private final Integer minimumHops;
    private final Integer maximumHops;
    private final PatternType patternType;
    private final Expression filterExpr;

    // We must be able to assign variables to our edges, as well as change the direction of UNDIRECTED edges.
    private VariableExpr variableExpr;
    private EdgeDirection edgeDirection;

    public EdgeDescriptor(EdgeDirection edgeDirection, PatternType patternType, Set<ElementLabel> edgeLabels,
            Expression filterExpr, VariableExpr variableExpr, Integer minimumHops, Integer maximumHops) {
        this.edgeLabels = edgeLabels;
        this.patternType = patternType;
        this.minimumHops = minimumHops;
        this.maximumHops = maximumHops;
        this.filterExpr = filterExpr;
        this.edgeDirection = edgeDirection;
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

    public Expression getFilterExpr() {
        return filterExpr;
    }

    public VariableExpr getVariableExpr() {
        return variableExpr;
    }

    public void setVariableExpr(VariableExpr variableExpr) {
        this.variableExpr = variableExpr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeDirection, patternType, edgeLabels, variableExpr, filterExpr, minimumHops, maximumHops);
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
                && Objects.equals(this.filterExpr, that.filterExpr)
                && Objects.equals(this.minimumHops, that.minimumHops)
                && Objects.equals(this.maximumHops, that.maximumHops);
    }

    @Override
    public String toString() {
        String labelsString = edgeLabels.stream().map(ElementLabel::toString).collect(Collectors.joining("|"));
        String variableString = (variableExpr != null) ? variableExpr.getVar().toString() : "";
        String minHopsString = ((minimumHops == null) ? "" : minimumHops.toString());
        String maxHopsString = ((maximumHops == null) ? "" : maximumHops.toString());
        String subPathString = (patternType != PatternType.PATH) ? "" : "{" + minHopsString + "," + maxHopsString + "}";
        String filterString = (filterExpr == null) ? "" : (" WHERE " + filterExpr + " ");
        return String.format("%s-[%s:(%s)%s%s]-%s", (edgeDirection == EdgeDirection.LEFT_TO_RIGHT) ? "" : "<",
                variableString, labelsString, subPathString, filterString,
                (edgeDirection == EdgeDirection.RIGHT_TO_LEFT) ? "" : ">");
    }

    public enum EdgeDirection {
        LEFT_TO_RIGHT("L2R"),
        RIGHT_TO_LEFT("R2L"),
        UNDIRECTED("U");

        // For printing purposes...
        private final String shortName;

        EdgeDirection(String shortName) {
            this.shortName = shortName;
        }

        @Override
        public String toString() {
            return shortName;
        }
    }

    public enum PatternType {
        PATH,
        EDGE
    }
}
