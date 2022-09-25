/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.algebra.translator;

import static org.apache.asterix.graphix.runtime.function.GraphixFunctionInfoCollection.*;

import java.util.Iterator;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.clause.extension.IGraphixVisitorExtension;
import org.apache.asterix.graphix.lang.clause.extension.LowerListClauseExtension;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseCollection;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.sqlpp.clause.AbstractBinaryCorrelateClause;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.translator.SqlppExpressionToPlanTranslator;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;

public class GraphixExpressionToPlanTranslator extends SqlppExpressionToPlanTranslator {
    public GraphixExpressionToPlanTranslator(MetadataProvider metadataProvider, int currentVarCounter,
            Map<VarIdentifier, IAObject> externalVars) throws AlgebricksException {
        super(metadataProvider, currentVarCounter, externalVars);
    }

    @Override
    public Pair<ILogicalOperator, LogicalVariable> visit(IVisitorExtension ve, Mutable<ILogicalOperator> tupSource)
            throws CompilationException {
        if (ve instanceof IGraphixVisitorExtension) {
            IGraphixVisitorExtension gve = (IGraphixVisitorExtension) ve;
            if (gve.getKind() != IGraphixVisitorExtension.Kind.LOWER_LIST) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                        "Encountered illegal type of Graphix visitor extension!");
            }
            return translateLowerListClause((LowerListClauseExtension) gve, tupSource);

        } else {
            return super.visit(ve, tupSource);
        }
    }

    public Pair<ILogicalOperator, LogicalVariable> translateLowerListClause(LowerListClauseExtension llce,
            Mutable<ILogicalOperator> tupSource) throws CompilationException {
        ClauseCollection clauseCollection = llce.getLowerListClause().getClauseCollection();
        Iterator<AbstractClause> clauseIterator = clauseCollection.iterator();
        if (!clauseIterator.hasNext()) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE,
                    "Encountered empty lower-clause collection!");
        }

        // Translate our leading clause in our collection.
        AbstractBinaryCorrelateClause leadingLowerClause = (AbstractBinaryCorrelateClause) clauseIterator.next();
        LogicalVariable leftVar = context.newVarFromExpression(leadingLowerClause.getRightVariable());
        Mutable<ILogicalOperator> topOpRef = new MutableObject<>();
        //        if (tupSource.getValue() instanceof GraphTupleSourceOperator) {
        //            // Do not ignore our condition.
        //            topOpRef = new MutableObject<>(leadingLowerClause.accept(this, tupSource).first);
        //
        //        } else {
        // Our first clause is functionally equivalent to the left expression of a FROM-TERM. Ignore our condition.
        Expression leftLangExpr = leadingLowerClause.getRightExpression();
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> eo = langExprToAlgExpression(leftLangExpr, tupSource);
        Pair<ILogicalExpression, Mutable<ILogicalOperator>> unnestExpr = makeUnnestExpression(eo.first, eo.second);
        UnnestOperator unnestOp = new UnnestOperator(leftVar, new MutableObject<>(unnestExpr.first));
        unnestOp.getInputs().add(unnestExpr.second);
        unnestOp.setSourceLocation(clauseCollection.getSourceLocation());
        topOpRef.setValue(unnestOp);
        //        }

        // The remainder of our clauses are either JOINs, UNNESTs, LETs, WHEREs, or GRAPH-CLAUSEs.
        while (clauseIterator.hasNext()) {
            AbstractClause workingLowerClause = clauseIterator.next();
            //            if (workingLowerClause instanceof LowerSwitchClause) {
            //                LowerSwitchClause lowerBFSClause = (LowerSwitchClause) workingLowerClause;
            //                LowerSwitchClauseExtension visitorExtension =
            //                        (LowerSwitchClauseExtension) lowerBFSClause.getVisitorExtension();
            //                topOpRef = new MutableObject<>(translateLowerGraphClause(visitorExtension, topOpRef).first);
            //
            //            } else {
            topOpRef = new MutableObject<>(workingLowerClause.accept(this, topOpRef).first);
            //            }
        }
        return new Pair<>(topOpRef.getValue(), leftVar);
    }
}
