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
package org.apache.asterix.graphix.function.rewrite;

import java.util.List;
import java.util.function.Function;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.record.IElementRecord;
import org.apache.asterix.graphix.lang.rewrites.record.PathRecord;
import org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.optype.UnnestType;

/**
 * Given the expression PATH_LABELS(myPathVar), rewrite this function to extract unique labels from the given path.
 * 1. First, we create a FROM-TERM from the given call argument. We assume that this is a path variable.
 * 2. Next, we build an ordered-list expression of LABEL accesses to the left vertex, edge, and right vertex of our
 * path record.
 * 3. UNNEST this ordered-list expression to get the label values, and mark the associated SELECT-EXPR as DISTINCT to
 * remove duplicates labels. Building this ordered-list and then UNNESTing said list is required to get a flat list of
 * label values.
 *
 * TODO (GLENN): We can definitely remove the "DISTINCT" and "UNNEST", but this is logically equivalent.
 */
public class PathLabelsFunctionRewrite implements IFunctionRewrite {
    @Override
    public Expression apply(GraphixRewritingContext graphixRewritingContext, List<Expression> callArguments)
            throws CompilationException {
        // Access the label field in each of edge record fields.
        VariableExpr edgeVar = new VariableExpr(graphixRewritingContext.getNewVariable());
        Function<String, FieldAccessor> labelAccessBuilder = i -> {
            final Identifier detailSuffix = new Identifier(IElementRecord.ELEMENT_DETAIL_NAME);
            final Identifier labelSuffix = new Identifier(IElementRecord.ELEMENT_LABEL_FIELD_NAME);
            FieldAccessor elementAccess = new FieldAccessor(edgeVar, new Identifier(i));
            FieldAccessor detailAccess = new FieldAccessor(elementAccess, detailSuffix);
            return new FieldAccessor(detailAccess, labelSuffix);
        };
        FieldAccessor leftVertexLabel = labelAccessBuilder.apply(PathRecord.LEFT_VERTEX_FIELD_NAME);
        FieldAccessor edgeLabel = labelAccessBuilder.apply(PathRecord.EDGE_FIELD_NAME);
        FieldAccessor rightVertexLabel = labelAccessBuilder.apply(PathRecord.RIGHT_VERTEX_FIELD_NAME);

        // Build a list of labels, and UNNEST this list to get a flat list of labels.
        ListConstructor listConstructor = new ListConstructor(ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR,
                List.of(leftVertexLabel, edgeLabel, rightVertexLabel));
        VariableExpr labelVar = new VariableExpr(graphixRewritingContext.getNewVariable());
        UnnestClause edgeUnnest = new UnnestClause(UnnestType.INNER, listConstructor, labelVar, null, null);
        FromTerm fromTerm = new FromTerm(callArguments.get(0), edgeVar, null, List.of(edgeUnnest));

        // Build the SELECT-EXPR. We will also mark this SELECT-CLAUSE as DISTINCT.
        SelectExpression selectExpr = LowerRewritingUtil.buildSelectWithFromAndElement(fromTerm, null, labelVar);
        selectExpr.getSelectSetOperation().getLeftInput().getSelectBlock().getSelectClause().setDistinct(true);
        return selectExpr;
    }
}
