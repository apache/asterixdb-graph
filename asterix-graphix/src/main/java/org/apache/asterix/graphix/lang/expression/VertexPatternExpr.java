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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.rewrites.visitor.IGraphixLangVisitor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * A query vertex (not to be confused with a vertex constructor) is composed of a set of labels (which may be empty)
 * and a variable (which may initially be null).
 */
public class VertexPatternExpr extends AbstractExpression {
    private final Set<ElementLabel> labels;
    private VariableExpr variableExpr;

    public VertexPatternExpr(VariableExpr variableExpr, Set<ElementLabel> labels) {
        this.variableExpr = variableExpr;
        this.labels = labels;
    }

    public Set<ElementLabel> getLabels() {
        return labels;
    }

    public VariableExpr getVariableExpr() {
        return variableExpr;
    }

    public void setVariableExpr(VariableExpr variableExpr) {
        this.variableExpr = variableExpr;
    }

    public List<GraphElementIdentifier> generateIdentifiers(GraphIdentifier graphIdentifier) {
        return labels.stream()
                .map(v -> new GraphElementIdentifier(graphIdentifier, GraphElementIdentifier.Kind.VERTEX, v))
                .collect(Collectors.toList());
    }

    @Override
    public int hashCode() {
        return Objects.hash(labels, variableExpr);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof VertexPatternExpr)) {
            return false;
        }
        VertexPatternExpr that = (VertexPatternExpr) object;
        return Objects.equals(this.labels, that.labels) && Objects.equals(this.variableExpr, that.variableExpr);
    }

    @Override
    public String toString() {
        String labelsString = labels.stream().map(ElementLabel::toString).collect(Collectors.joining("|"));
        String variableString = (variableExpr != null) ? variableExpr.getVar().toString() : "";
        return String.format("(%s:%s)", variableString, labelsString);
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
