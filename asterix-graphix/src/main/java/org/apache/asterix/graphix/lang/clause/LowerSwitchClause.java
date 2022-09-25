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
package org.apache.asterix.graphix.lang.clause;

import java.util.Objects;

import org.apache.asterix.graphix.algebra.compiler.option.SemanticsNavigationOption;
import org.apache.asterix.graphix.lang.clause.extension.LowerSwitchClauseExtension;
import org.apache.asterix.graphix.lang.rewrite.lower.action.MatchSemanticAction;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.CollectionTable;
import org.apache.asterix.graphix.lang.struct.ElementLabel;
import org.apache.asterix.lang.common.base.AbstractExtensionClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.IntegerLiteral;

/**
 * A functional equivalent to the {@link org.apache.asterix.lang.sqlpp.clause.JoinClause}, used as a container for
 * lowering a recursive / ambiguous portion of a {@link FromGraphClause}.
 */
public class LowerSwitchClause extends AbstractExtensionClause {
    private final LowerSwitchClauseExtension lowerClauseExtension;
    private final ClauseOutputEnvironment clauseOutputEnvironment;
    private final ClauseInputEnvironment clauseInputEnvironment;
    private final CollectionTable collectionLookupTable;

    /**
     * The following is set by {@link MatchSemanticAction}.
     */
    private SemanticsNavigationOption navigationSemantics;

    /**
     * The output to a BFS clause will be of type list, containing three items.
     */
    public static class ClauseOutputEnvironment {
        public static final int OUTPUT_VERTEX_ITERATION_VARIABLE_INDEX = 0;
        public static final int OUTPUT_VERTEX_JOIN_VARIABLE_INDEX = 1;
        public static final int OUTPUT_PATH_VARIABLE_INDEX = 2;
        private final VariableExpr outputVariable;
        private final ElementLabel endingLabel;

        // We provide the following as output, through our output variable.
        private final VariableExpr outputVertexIterationVariable;
        private final VariableExpr outputVertexJoinVariable;
        private final VariableExpr pathVariable;

        public ClauseOutputEnvironment(VariableExpr outputVariable, VariableExpr outputVertexIterationVariable,
                VariableExpr outputVertexJoinVariable, VariableExpr pathVariable, ElementLabel endingLabel) {
            this.outputVariable = Objects.requireNonNull(outputVariable);
            this.outputVertexIterationVariable = Objects.requireNonNull(outputVertexIterationVariable);
            this.outputVertexJoinVariable = Objects.requireNonNull(outputVertexJoinVariable);
            this.pathVariable = Objects.requireNonNull(pathVariable);
            this.endingLabel = endingLabel;
        }

        public VariableExpr getOutputVariable() {
            return outputVariable;
        }

        public VariableExpr getOutputVertexIterationVariable() {
            return outputVertexIterationVariable;
        }

        public VariableExpr getOutputVertexJoinVariable() {
            return outputVertexJoinVariable;
        }

        public VariableExpr getPathVariable() {
            return pathVariable;
        }

        public ElementLabel getEndingLabel() {
            return endingLabel;
        }

        public Expression buildIterationVariableAccess() {
            LiteralExpr indexExpr = new LiteralExpr(new IntegerLiteral(OUTPUT_VERTEX_ITERATION_VARIABLE_INDEX));
            return new IndexAccessor(outputVariable, IndexAccessor.IndexKind.ELEMENT, indexExpr);
        }

        public Expression buildJoinVariableAccess() {
            LiteralExpr indexExpr = new LiteralExpr(new IntegerLiteral(OUTPUT_VERTEX_JOIN_VARIABLE_INDEX));
            return new IndexAccessor(outputVariable, IndexAccessor.IndexKind.ELEMENT, indexExpr);
        }

        public Expression buildPathVariableAccess() {
            LiteralExpr indexExpr = new LiteralExpr(new IntegerLiteral(OUTPUT_PATH_VARIABLE_INDEX));
            return new IndexAccessor(outputVariable, IndexAccessor.IndexKind.ELEMENT, indexExpr);
        }

        @Override
        public int hashCode() {
            return Objects.hash(outputVariable, outputVertexIterationVariable, outputVertexJoinVariable, pathVariable);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof ClauseOutputEnvironment)) {
                return false;
            }
            ClauseOutputEnvironment that = (ClauseOutputEnvironment) object;
            return Objects.equals(this.outputVariable, that.outputVariable)
                    && Objects.equals(this.outputVertexIterationVariable, that.outputVertexIterationVariable)
                    && Objects.equals(this.outputVertexJoinVariable, that.outputVertexJoinVariable)
                    && Objects.equals(this.pathVariable, that.pathVariable)
                    && Objects.equals(this.endingLabel, that.endingLabel);
        }

        @Override
        public String toString() {
            String exposeToOutputString = "clause-output-env (" + outputVariable.toString() + ")";
            String iterationString = "iteration: " + outputVertexIterationVariable.toString();
            String joinString = "join: " + outputVertexJoinVariable.toString();
            String pathString = "path: " + pathVariable.toString();
            return String.format("%s: {%s, %s, %s}", exposeToOutputString, iterationString, joinString, pathString);
        }
    }

    /**
     * The input to a BFS clause will be a single representative vertex.
     */
    public static class ClauseInputEnvironment {
        private final VariableExpr inputVariable;
        private final ElementLabel startingLabel;

        public ClauseInputEnvironment(VariableExpr inputVariable, ElementLabel startingLabel) {
            this.inputVariable = inputVariable;
            this.startingLabel = startingLabel;
        }

        public VariableExpr getInputVariable() {
            return inputVariable;
        }

        public ElementLabel getStartingLabel() {
            return startingLabel;
        }

        @Override
        public int hashCode() {
            return Objects.hash(inputVariable, startingLabel);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof ClauseInputEnvironment)) {
                return false;
            }
            ClauseInputEnvironment that = (ClauseInputEnvironment) object;
            return Objects.equals(this.inputVariable, that.inputVariable)
                    && Objects.equals(this.startingLabel, that.startingLabel);
        }
    }

    public LowerSwitchClause(CollectionTable pathClauseCollectionTable, ClauseInputEnvironment inputEnvironment,
            ClauseOutputEnvironment outputEnvironment) {
        this.collectionLookupTable = pathClauseCollectionTable;
        this.clauseInputEnvironment = inputEnvironment;
        this.clauseOutputEnvironment = outputEnvironment;
        this.lowerClauseExtension = new LowerSwitchClauseExtension(this);
    }

    public void setNavigationSemantics(SemanticsNavigationOption navigationSemantics) {
        this.navigationSemantics = navigationSemantics;
    }

    public SemanticsNavigationOption getNavigationSemantics() {
        return navigationSemantics;
    }

    public ClauseInputEnvironment getClauseInputEnvironment() {
        return clauseInputEnvironment;
    }

    public ClauseOutputEnvironment getClauseOutputEnvironment() {
        return clauseOutputEnvironment;
    }

    public CollectionTable getCollectionLookupTable() {
        return collectionLookupTable;
    }

    @Override
    public IVisitorExtension getVisitorExtension() {
        return lowerClauseExtension;
    }

    @Override
    public int hashCode() {
        return Objects.hash(collectionLookupTable, clauseInputEnvironment, clauseOutputEnvironment,
                navigationSemantics);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LowerSwitchClause)) {
            return false;
        }
        LowerSwitchClause that = (LowerSwitchClause) object;
        return Objects.equals(this.collectionLookupTable, that.collectionLookupTable)
                && Objects.equals(this.clauseOutputEnvironment, that.clauseOutputEnvironment)
                && Objects.equals(this.clauseInputEnvironment, that.clauseInputEnvironment)
                && Objects.equals(this.navigationSemantics, that.navigationSemantics);
    }

    @Override
    public String toString() {
        return clauseOutputEnvironment.toString();
    }
}
