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
import java.util.UUID;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrites.visitor.IGraphixLangVisitor;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.AbstractExpression;
import org.apache.asterix.lang.common.base.AbstractLangExpression;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * An expression which describes the schema of a graph, containing a list of vertices ({@link VertexConstructor}) and
 * a list of edges ({@link EdgeConstructor}) that connect the aforementioned vertices.
 */
public class GraphConstructor extends AbstractExpression implements IGraphExpr {
    private final List<VertexConstructor> vertexConstructors;
    private final List<EdgeConstructor> edgeConstructors;

    // On parsing, we want to identify which graph-constructors are unique from one another.
    private final UUID instanceID;

    public GraphConstructor(List<VertexConstructor> vertexConstructors, List<EdgeConstructor> edgeConstructors) {
        this.vertexConstructors = vertexConstructors;
        this.edgeConstructors = edgeConstructors;
        this.instanceID = UUID.randomUUID();
    }

    public List<VertexConstructor> getVertexElements() {
        return vertexConstructors;
    }

    public List<EdgeConstructor> getEdgeElements() {
        return edgeConstructors;
    }

    public String getInstanceID() {
        return instanceID.toString();
    }

    @Override
    public Kind getKind() {
        return null;
    }

    @Override
    public GraphExprKind getGraphExprKind() {
        return GraphExprKind.GRAPH_CONSTRUCTOR;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexConstructors, edgeConstructors);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphConstructor)) {
            return false;
        }
        GraphConstructor that = (GraphConstructor) object;
        return vertexConstructors.equals(that.vertexConstructors) && edgeConstructors.equals(that.edgeConstructors);
    }

    /**
     * A vertex constructor (not be confused with a query vertex) is composed of the following:
     * - An AST containing the vertex body expression, as well as the raw body string itself.
     * - A single label that groups the aforementioned body with other vertices of the same label.
     * - A list of primary key fields, which must be the same as other vertices of the same label. These fields are
     * used in the JOIN clause with edges.
     */
    public static class VertexConstructor extends AbstractLangExpression {
        private final List<Integer> primaryKeySourceIndicators;
        private final List<List<String>> primaryKeyFields;
        private final Expression expression;
        private final ElementLabel label;
        private final String definition;

        public VertexConstructor(ElementLabel label, List<List<String>> primaryKeyFields,
                List<Integer> primaryKeySourceIndicators, Expression expression, String definition) {
            this.primaryKeySourceIndicators = primaryKeySourceIndicators;
            this.primaryKeyFields = primaryKeyFields;
            this.expression = expression;
            this.definition = definition;
            this.label = label;
        }

        public List<List<String>> getPrimaryKeyFields() {
            return primaryKeyFields;
        }

        public List<Integer> getPrimaryKeySourceIndicators() {
            return primaryKeySourceIndicators;
        }

        public Expression getExpression() {
            return expression;
        }

        public String getDefinition() {
            return definition;
        }

        public ElementLabel getLabel() {
            return label;
        }

        @Override
        public String toString() {
            return "(:" + label + ") AS " + definition;
        }

        @Override
        public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
            return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
        }

        @Override
        public int hashCode() {
            return Objects.hash(primaryKeyFields, expression, definition, label);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof VertexConstructor)) {
                return false;
            }
            VertexConstructor that = (VertexConstructor) object;
            return primaryKeySourceIndicators.equals(that.primaryKeySourceIndicators)
                    && primaryKeyFields.equals(that.primaryKeyFields) && expression.equals(that.expression)
                    && definition.equals(that.definition) && label.equals(that.label);
        }
    }

    /**
     * An edge constructor (not be confused with a query edge) is composed of the following:
     * - An AST containing the edge body expression, as well as the raw body string itself.
     * - A single edge label that groups the aforementioned body with other edges of the same label.
     * - A single label that denotes the source vertices of this edge, as well as another label that denotes the
     * destination vertices of this edge.
     * - A list of source key fields, which must be the same as other edges of the same label. These fields are used in
     * the JOIN clause with the corresponding source vertices.
     * - A list of destination key fields, which must be the same as other edges of the same label. These fields are
     * used in the JOIN clause with the corresponding destination vertices.
     */
    public static class EdgeConstructor extends AbstractLangExpression {
        private final List<Integer> destinationKeySourceIndicators;
        private final List<Integer> sourceKeySourceIndicators;

        private final List<List<String>> destinationKeyFields;
        private final List<List<String>> sourceKeyFields;

        private final ElementLabel destinationLabel, edgeLabel, sourceLabel;
        private final Expression expression;
        private final String definition;

        public EdgeConstructor(ElementLabel edgeLabel, ElementLabel destinationLabel, ElementLabel sourceLabel,
                List<List<String>> destinationKeyFields, List<Integer> destinationKeySourceIndicators,
                List<List<String>> sourceKeyFields, List<Integer> sourceKeySourceIndicators, Expression expression,
                String definition) {
            this.destinationKeySourceIndicators = destinationKeySourceIndicators;
            this.sourceKeySourceIndicators = sourceKeySourceIndicators;
            this.destinationKeyFields = destinationKeyFields;
            this.sourceKeyFields = sourceKeyFields;
            this.destinationLabel = destinationLabel;
            this.edgeLabel = edgeLabel;
            this.sourceLabel = sourceLabel;
            this.expression = expression;
            this.definition = definition;
        }

        public List<Integer> getDestinationKeySourceIndicators() {
            return destinationKeySourceIndicators;
        }

        public List<Integer> getSourceKeySourceIndicators() {
            return sourceKeySourceIndicators;
        }

        public List<List<String>> getDestinationKeyFields() {
            return destinationKeyFields;
        }

        public List<List<String>> getSourceKeyFields() {
            return sourceKeyFields;
        }

        public ElementLabel getDestinationLabel() {
            return destinationLabel;
        }

        public ElementLabel getEdgeLabel() {
            return edgeLabel;
        }

        public ElementLabel getSourceLabel() {
            return sourceLabel;
        }

        public Expression getExpression() {
            return expression;
        }

        public String getDefinition() {
            return definition;
        }

        @Override
        public String toString() {
            String edgeBodyPattern = "[:" + edgeLabel + "]";
            String sourceNodePattern = "(:" + sourceLabel + ")";
            String destinationNodePattern = "(:" + destinationLabel + ")";
            String edgePattern = sourceNodePattern + "-" + edgeBodyPattern + "->" + destinationNodePattern;
            return edgePattern + " AS " + definition;
        }

        @Override
        public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
            return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
        }

        @Override
        public int hashCode() {
            return Objects.hash(destinationKeyFields, sourceKeyFields, destinationLabel, edgeLabel, sourceLabel,
                    expression, definition);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof EdgeConstructor)) {
                return false;
            }
            EdgeConstructor that = (EdgeConstructor) object;
            return destinationKeySourceIndicators.equals(that.destinationKeySourceIndicators)
                    && sourceKeySourceIndicators.equals(that.sourceKeySourceIndicators)
                    && destinationKeyFields.equals(that.destinationKeyFields)
                    && sourceKeyFields.equals(that.sourceKeyFields) && destinationLabel.equals(that.destinationLabel)
                    && edgeLabel.equals(that.edgeLabel) && sourceLabel.equals(that.sourceLabel)
                    && expression.equals(that.expression) && definition.equals(that.definition);
        }
    }
}
