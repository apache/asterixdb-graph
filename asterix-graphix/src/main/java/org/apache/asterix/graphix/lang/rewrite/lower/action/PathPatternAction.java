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
package org.apache.asterix.graphix.lang.rewrite.lower.action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.lower.LoweringEnvironment;
import org.apache.asterix.graphix.type.MaterializePathTypeComputer;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.literal.StringLiteral;

/**
 * Build a path record out of a {@link PathPatternExpr} instance. This record contains two fields: "Vertices", which
 * contains a list of all representative vertex variables, and "Edges", which contains a list of all representative
 * edge variables.
 */
public class PathPatternAction implements IEnvironmentAction {
    private final PathPatternExpr pathPatternExpr;

    public PathPatternAction(PathPatternExpr pathPatternExpr) {
        this.pathPatternExpr = pathPatternExpr;
    }

    @Override
    public void apply(LoweringEnvironment loweringEnvironment) throws CompilationException {
        if (pathPatternExpr.getVariableExpr() != null) {
            loweringEnvironment.acceptTransformer(lowerList -> {
                // We have a named non-sub-path.
                RecordConstructor recordConstructor = new RecordConstructor();
                recordConstructor.setSourceLocation(pathPatternExpr.getSourceLocation());
                List<VertexPatternExpr> vertexExpressions = pathPatternExpr.getVertexExpressions();
                List<EdgePatternExpr> edgeExpressions = pathPatternExpr.getEdgeExpressions();
                buildPathRecord(vertexExpressions, edgeExpressions, recordConstructor);
                lowerList.addPathBinding(pathPatternExpr.getVariableExpr(), recordConstructor);
            });
        }
        for (LetClause reboundSubPathExpression : pathPatternExpr.getReboundSubPathList()) {
            loweringEnvironment.acceptTransformer(lowerList -> {
                // We have sub-paths we need to introduce.
                Expression reboundExpression = reboundSubPathExpression.getBindingExpr();
                lowerList.addPathBinding(reboundSubPathExpression.getVarExpr(), reboundExpression);
            });
        }
    }

    public static void buildPathRecord(Collection<VertexPatternExpr> vertexExpressions,
            Collection<EdgePatternExpr> edgeExpressions, RecordConstructor outputRecordConstructor) {
        List<FieldBinding> fieldBindingList = new ArrayList<>();

        // Assemble our vertices into a list.
        List<Expression> vertexVariableExprList = vertexExpressions.stream().map(VertexPatternExpr::getVariableExpr)
                .distinct().collect(Collectors.toList());
        ListConstructor vertexVariableList = new ListConstructor();
        vertexVariableList.setType(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR);
        vertexVariableList.setExprList(vertexVariableExprList);
        vertexVariableList.setSourceLocation(outputRecordConstructor.getSourceLocation());
        StringLiteral verticesFieldNameLiteral = new StringLiteral(MaterializePathTypeComputer.VERTICES_FIELD_NAME);
        LiteralExpr verticesFieldName = new LiteralExpr(verticesFieldNameLiteral);
        fieldBindingList.add(new FieldBinding(verticesFieldName, vertexVariableList));

        // Assemble our edges into a list.
        List<Expression> edgeVariableExprList =
                edgeExpressions.stream().map(e -> e.getEdgeDescriptor().getVariableExpr()).collect(Collectors.toList());
        ListConstructor edgeVariableList = new ListConstructor();
        edgeVariableList.setType(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR);
        edgeVariableList.setExprList(edgeVariableExprList);
        edgeVariableList.setSourceLocation(outputRecordConstructor.getSourceLocation());
        StringLiteral edgesFieldNameLiteral = new StringLiteral(MaterializePathTypeComputer.EDGES_FIELD_NAME);
        LiteralExpr edgesFieldName = new LiteralExpr(edgesFieldNameLiteral);
        fieldBindingList.add(new FieldBinding(edgesFieldName, edgeVariableList));

        // Set our field bindings in our output record constructor.
        outputRecordConstructor.setFbList(fieldBindingList);
    }
}
