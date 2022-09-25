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
package org.apache.asterix.graphix.lang.expression;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.EdgeIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.graphix.lang.visitor.base.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * A query edge (not to be confused with an edge constructor) is composed of:
 * <ul>
 *  <li>A {@link EdgeDescriptor} (containing the edge labels, an optional edge variable, and the hop range).</li>
 *  <li>An optional internal {@link VertexPatternExpr}.</li>
 *  <li>A left {@link VertexPatternExpr}.</li>
 *  <li>A right {@link VertexPatternExpr}.</li>
 * </ul>
 */
public class EdgePatternExpr extends AbstractExpression {
    private final EdgeDescriptor edgeDescriptor;
    private VertexPatternExpr leftVertex;
    private VertexPatternExpr rightVertex;
    private VertexPatternExpr internalVertex;

    public EdgePatternExpr(VertexPatternExpr leftVertex, VertexPatternExpr rightVertex, EdgeDescriptor edgeDescriptor) {
        this.leftVertex = Objects.requireNonNull(leftVertex);
        this.rightVertex = Objects.requireNonNull(rightVertex);
        this.edgeDescriptor = Objects.requireNonNull(edgeDescriptor);
        if (edgeDescriptor.getPatternType() == EdgeDescriptor.PatternType.PATH) {
            // If we have a sub-path, we have an internal vertex that we need to manage.
            this.internalVertex = new VertexPatternExpr(null, null, new HashSet<>());

        } else {
            this.internalVertex = null;
        }
    }

    public VertexPatternExpr getLeftVertex() {
        return leftVertex;
    }

    public VertexPatternExpr getRightVertex() {
        return rightVertex;
    }

    public VertexPatternExpr getInternalVertex() {
        return internalVertex;
    }

    public EdgeDescriptor getEdgeDescriptor() {
        return edgeDescriptor;
    }

    public void setLeftVertex(VertexPatternExpr leftVertex) {
        this.leftVertex = leftVertex;
    }

    public void setRightVertex(VertexPatternExpr rightVertex) {
        this.rightVertex = rightVertex;
    }

    public void setInternalVertex(VertexPatternExpr internalVertex) {
        this.internalVertex = internalVertex;
    }

    public List<EdgeIdentifier> generateIdentifiers(GraphIdentifier graphIdentifier) {
        List<EdgeIdentifier> edgeIdentifiers = new ArrayList<>();
        for (ElementLabel leftLabel : leftVertex.getLabels()) {
            for (ElementLabel rightLabel : rightVertex.getLabels()) {
                for (ElementLabel edgeLabel : edgeDescriptor.getEdgeLabels()) {
                    if (edgeDescriptor.getEdgeDirection() != EdgeDescriptor.EdgeDirection.RIGHT_TO_LEFT) {
                        edgeIdentifiers.add(new EdgeIdentifier(graphIdentifier, leftLabel, edgeLabel, rightLabel));
                    }
                    if (edgeDescriptor.getEdgeDirection() != EdgeDescriptor.EdgeDirection.LEFT_TO_RIGHT) {
                        edgeIdentifiers.add(new EdgeIdentifier(graphIdentifier, rightLabel, edgeLabel, leftLabel));
                    }
                }
            }
        }
        return edgeIdentifiers;
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftVertex, rightVertex, internalVertex, edgeDescriptor);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof EdgePatternExpr)) {
            return false;
        }
        EdgePatternExpr that = (EdgePatternExpr) object;
        return Objects.equals(this.leftVertex, that.leftVertex) && Objects.equals(this.rightVertex, that.rightVertex)
                && Objects.equals(this.internalVertex, that.internalVertex)
                && Objects.equals(this.edgeDescriptor, that.edgeDescriptor);
    }

    @Override
    public String toString() {
        return leftVertex.toString() + edgeDescriptor.toString() + rightVertex.toString();
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }
}
