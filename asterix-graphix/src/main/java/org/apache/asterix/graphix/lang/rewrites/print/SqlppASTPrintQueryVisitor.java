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
package org.apache.asterix.graphix.lang.rewrites.print;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.clause.LimitClause;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.clause.WhereClause;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.GbyVariableExpressionPair;
import org.apache.asterix.lang.common.expression.IfExpr;
import org.apache.asterix.lang.common.expression.IndexAccessor;
import org.apache.asterix.lang.common.expression.ListConstructor;
import org.apache.asterix.lang.common.expression.ListSliceExpression;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.OperatorExpr;
import org.apache.asterix.lang.common.expression.QuantifiedExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.UnaryExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.lang.common.struct.OperatorType;
import org.apache.asterix.lang.sqlpp.clause.FromClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.HavingClause;
import org.apache.asterix.lang.sqlpp.clause.JoinClause;
import org.apache.asterix.lang.sqlpp.clause.NestClause;
import org.apache.asterix.lang.sqlpp.clause.Projection;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.clause.SelectElement;
import org.apache.asterix.lang.sqlpp.clause.SelectRegular;
import org.apache.asterix.lang.sqlpp.clause.SelectSetOperation;
import org.apache.asterix.lang.sqlpp.clause.UnnestClause;
import org.apache.asterix.lang.sqlpp.expression.CaseExpression;
import org.apache.asterix.lang.sqlpp.expression.SelectExpression;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.optype.SetOpType;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppQueryExpressionVisitor;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * Visitor class to create a valid SQL++ query out of a **valid** AST. We make the following assumptions:
 * 1. The entry point is a {@link Query}, which we will print to.
 * 2. All {@link GroupbyClause} nodes only have one set of {@link GbyVariableExpressionPair} (i.e. the GROUP-BY
 * rewrites should have fired).
 * 3. No positional variables are included.
 * 4. No {@link WindowExpression} nodes are included (this is on the TODO list).
 */
public class SqlppASTPrintQueryVisitor extends AbstractSqlppQueryExpressionVisitor<String, Void> {
    private final PrintWriter printWriter;

    public SqlppASTPrintQueryVisitor(PrintWriter printWriter) {
        this.printWriter = printWriter;
    }

    @Override
    public String visit(Query query, Void arg) throws CompilationException {
        String queryBodyString = query.getBody().accept(this, arg) + ";";
        queryBodyString = queryBodyString.trim().replaceAll("\\s+", " ");
        printWriter.print(queryBodyString);
        return null;
    }

    @Override
    public String visit(SelectExpression selectStatement, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(" ( ");
        if (selectStatement.hasLetClauses()) {
            sb.append(" LET ");
            sb.append(selectStatement.getLetList().stream().map(this::visitAndSwallowException)
                    .collect(Collectors.joining(", ")));
        }
        sb.append(selectStatement.getSelectSetOperation().accept(this, arg));
        if (selectStatement.hasOrderby()) {
            sb.append(selectStatement.getOrderbyClause().accept(this, arg));
        }
        if (selectStatement.hasLimit()) {
            sb.append(selectStatement.getLimitClause().accept(this, arg));
        }
        sb.append(" ) ");
        return sb.toString();
    }

    @Override
    public String visit(LetClause letClause, Void arg) throws CompilationException {
        return String.format(" %s =  ( %s ) ", letClause.getVarExpr().accept(this, arg),
                letClause.getBindingExpr().accept(this, arg));
    }

    @Override
    public String visit(SelectSetOperation selectSetOperation, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(selectSetOperation.getLeftInput().accept(this, arg));
        if (selectSetOperation.hasRightInputs()) {
            sb.append(selectSetOperation.getRightInputs().stream().map(s -> {
                // Ignore everything else but UNION-ALL.
                if (s.getSetOpType() == SetOpType.UNION) {
                    SetOperationInput rightInput = s.getSetOperationRightInput();
                    if (!s.isSetSemantics()) {
                        if (rightInput.selectBlock()) {
                            return " UNION ALL " + visitAndSwallowException(rightInput.getSelectBlock());
                        }
                        if (rightInput.subquery()) {
                            return " UNION ALL " + visitAndSwallowException(rightInput.getSubquery());
                        }
                    }
                }
                throw new NotImplementedException("Given SET operation is not implemented.");
            }).collect(Collectors.joining()));
        }
        return sb.toString();
    }

    @Override
    public String visit(SelectBlock selectBlock, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(visitAndSwallowException(selectBlock.getSelectClause()));
        if (selectBlock.hasFromClause()) {
            sb.append(selectBlock.getFromClause().accept(this, arg));
        }
        if (selectBlock.hasLetWhereClauses()) {
            List<LetClause> letClauses = selectBlock.getLetWhereList().stream()
                    .filter(c -> c.getClauseType() == Clause.ClauseType.LET_CLAUSE).map(c -> (LetClause) c)
                    .collect(Collectors.toList());
            List<WhereClause> whereClauses = selectBlock.getLetWhereList().stream()
                    .filter(c -> c.getClauseType() == Clause.ClauseType.WHERE_CLAUSE).map(c -> (WhereClause) c)
                    .collect(Collectors.toList());
            if (!letClauses.isEmpty()) {
                sb.append(" LET ");
                sb.append(letClauses.stream().map(this::visitAndSwallowException).collect(Collectors.joining(", ")));
            }
            if (!whereClauses.isEmpty()) {
                sb.append(" WHERE ");
                sb.append(
                        whereClauses.stream().map(this::visitAndSwallowException).collect(Collectors.joining(" AND ")));
            }
        }
        if (selectBlock.hasGroupbyClause()) {
            sb.append(selectBlock.getGroupbyClause().accept(this, arg));
        }
        if (selectBlock.hasLetHavingClausesAfterGroupby()) {
            List<LetClause> letClauses = selectBlock.getLetHavingListAfterGroupby().stream()
                    .filter(c -> c.getClauseType() == Clause.ClauseType.LET_CLAUSE).map(c -> (LetClause) c)
                    .collect(Collectors.toList());
            List<HavingClause> havingClauses = selectBlock.getLetHavingListAfterGroupby().stream()
                    .filter(c -> c.getClauseType() == Clause.ClauseType.HAVING_CLAUSE).map(c -> (HavingClause) c)
                    .collect(Collectors.toList());
            if (!letClauses.isEmpty()) {
                sb.append(" LET ");
                sb.append(letClauses.stream().map(this::visitAndSwallowException).collect(Collectors.joining(", ")));
            }
            if (!havingClauses.isEmpty()) {
                sb.append(" HAVING ");
                sb.append(havingClauses.stream().map(this::visitAndSwallowException).collect(Collectors.joining(", ")));
            }
        }
        return sb.toString();
    }

    @Override
    public String visit(FromClause fromClause, Void arg) {
        return String.format(" FROM %s ", fromClause.getFromTerms().stream().map(this::visitAndSwallowException)
                .collect(Collectors.joining(", ")));
    }

    @Override
    public String visit(FromTerm fromTerm, Void arg) throws CompilationException {
        // Note: we do not include positional variables here.
        StringBuilder sb = new StringBuilder();
        sb.append(fromTerm.getLeftExpression().accept(this, arg));
        if (fromTerm.getLeftVariable() != null) {
            sb.append(" AS ");
            sb.append(fromTerm.getLeftVariable().accept(this, arg));
        }
        sb.append(fromTerm.getCorrelateClauses().stream().map(this::visitAndSwallowException)
                .collect(Collectors.joining()));
        return sb.toString();
    }

    @Override
    public String visit(WhereClause whereClause, Void arg) throws CompilationException {
        return whereClause.getWhereExpr().accept(this, arg);
    }

    @Override
    public String visit(GroupbyClause groupbyClause, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(" GROUP BY "); // Note: we should have rewritten grouping sets by now.
        sb.append(groupbyClause.getGbyPairList().stream().flatMap(Collection::stream).map(p -> {
            if (p.getVar() != null) {
                return " ( " + visitAndSwallowException(p.getExpr()) + " AS " + visitAndSwallowException(p.getVar())
                        + ")";

            } else {
                return " ( " + visitAndSwallowException(p.getExpr()) + " ) ";
            }
        }).collect(Collectors.joining(", ")));
        if (groupbyClause.hasGroupVar()) {
            sb.append(" GROUP AS ");
            sb.append(groupbyClause.getGroupVar().accept(this, arg));
            if (groupbyClause.hasGroupFieldList()) {
                sb.append(" ( ");
                sb.append(
                        groupbyClause.getGroupFieldList().stream()
                                .map(f -> visitAndSwallowException(f.first) + " AS "
                                        + formatIdentifierForQuery(f.second.getValue()))
                                .collect(Collectors.joining(", ")));
                sb.append(" ) ");
            }
        }
        return sb.toString();
    }

    @Override
    public String visit(HavingClause havingClause, Void arg) throws CompilationException {
        return havingClause.getFilterExpression().accept(this, arg);
    }

    @Override
    public String visit(SelectClause selectClause, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(" SELECT ");
        if (selectClause.distinct()) {
            sb.append(" DISTINCT ");
        }
        if (selectClause.selectElement()) {
            sb.append(" VALUE ");
            sb.append(selectClause.getSelectElement().accept(this, arg));

        } else {
            sb.append(selectClause.getSelectRegular().accept(this, arg));
        }
        return sb.toString();
    }

    @Override
    public String visit(SelectElement selectElement, Void arg) throws CompilationException {
        return selectElement.getExpression().accept(this, arg);
    }

    @Override
    public String visit(SelectRegular selectRegular, Void arg) {
        return selectRegular.getProjections().stream().map(this::visitAndSwallowException)
                .collect(Collectors.joining(", "));
    }

    @Override
    public String visit(Projection projection, Void arg) throws CompilationException {
        switch (projection.getKind()) {
            case NAMED_EXPR:
                return projection.getExpression().accept(this, arg) + " AS `"
                        + formatIdentifierForQuery(projection.getName()) + "`";
            case STAR:
                return "*";
            case VAR_STAR:
                return projection.getExpression().accept(this, arg) + ".*";
            default:
                throw new NotImplementedException("Illegal projection type encountered!");
        }
    }

    @Override
    public String visit(JoinClause joinClause, Void arg) throws CompilationException {
        // Note: we do not include positional variables here.
        StringBuilder sb = new StringBuilder();
        switch (joinClause.getJoinType()) {
            case INNER:
                sb.append(" INNER JOIN ");
                break;
            case LEFTOUTER:
                sb.append(" LEFT OUTER JOIN ");
                break;
            case RIGHTOUTER:
                sb.append(" RIGHT OUTER JOIN ");
                break;
        }
        sb.append(joinClause.getRightExpression().accept(this, arg));
        if (joinClause.getRightVariable() != null) {
            sb.append(" AS ");
            sb.append(joinClause.getRightVariable().accept(this, arg));
        }
        sb.append(" ON ");
        sb.append(joinClause.getConditionExpression().accept(this, arg));
        return sb.toString();
    }

    @Override
    public String visit(NestClause nestClause, Void arg) {
        throw new NotImplementedException("NEST clause not implemented!");
    }

    @Override
    public String visit(UnnestClause unnestClause, Void arg) throws CompilationException {
        // Note: we do not include positional variables here.
        StringBuilder sb = new StringBuilder();
        switch (unnestClause.getUnnestType()) {
            case INNER:
                sb.append(" UNNEST ");
                break;
            case LEFTOUTER:
                sb.append(" LEFT OUTER UNNEST ");
                break;
        }
        sb.append(unnestClause.getRightExpression().accept(this, arg));
        if (unnestClause.getRightVariable() != null) {
            sb.append(" AS ");
            sb.append(unnestClause.getRightVariable().accept(this, arg));
        }
        return sb.toString();
    }

    @Override
    public String visit(OrderbyClause orderbyClause, Void arg) {
        String orderByString = IntStream.range(0, orderbyClause.getOrderbyList().size()).mapToObj(i -> {
            // Build the order-modifier as a string.
            OrderbyClause.OrderModifier orderModifier = orderbyClause.getModifierList().get(i);
            String orderModifierAsString = (orderModifier == null) ? "" : orderModifier.toString();

            // Build our null-order-modifier as a string.
            OrderbyClause.NullOrderModifier nullOrderModifier = orderbyClause.getNullModifierList().get(i);
            String nullOrderModifierAsString = "";
            if (nullOrderModifier == OrderbyClause.NullOrderModifier.FIRST) {
                nullOrderModifierAsString = "NULLS FIRST";
            } else if (nullOrderModifier == OrderbyClause.NullOrderModifier.LAST) {
                nullOrderModifierAsString = "NULLS LAST";
            }

            // Finally, return the string expression.
            Expression orderByExpr = orderbyClause.getOrderbyList().get(i);
            return visitAndSwallowException(orderByExpr) + " " + orderModifierAsString + " " + nullOrderModifierAsString
                    + " ";
        }).collect(Collectors.joining(", "));
        return String.format(" ORDER BY %s ", orderByString);
    }

    @Override
    public String visit(LimitClause limitClause, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        if (limitClause.hasLimitExpr()) {
            sb.append(" LIMIT ");
            sb.append(limitClause.getLimitExpr().accept(this, arg));
        }
        if (limitClause.hasOffset()) {
            sb.append(" OFFSET ");
            sb.append(limitClause.getOffset().accept(this, arg));
        }
        return sb.toString();
    }

    @Override
    public String visit(CallExpr callExpr, Void arg) throws CompilationException {
        FunctionIdentifier functionIdentifier = callExpr.getFunctionSignature().createFunctionIdentifier();
        if (functionIdentifier.equals(BuiltinFunctions.DATASET)) {
            // We need to undo the DATASET call here.
            LiteralExpr dataverseNameExpr = (LiteralExpr) callExpr.getExprList().get(0);
            LiteralExpr datasetNameExpr = (LiteralExpr) callExpr.getExprList().get(1);
            String dataverseName = dataverseNameExpr.getValue().getStringValue();
            String datasetName = datasetNameExpr.getValue().getStringValue();
            return String.format("`%s`.`%s`", dataverseName, datasetName);

        } else if (functionIdentifier.equals(BuiltinFunctions.IS_MISSING)) {
            return String.format(" %s IS MISSING ", callExpr.getExprList().get(0).accept(this, arg));

        } else if (functionIdentifier.equals(BuiltinFunctions.IS_NULL)) {
            return String.format(" %s IS NULL ", callExpr.getExprList().get(0).accept(this, arg));

        } else if (functionIdentifier.equals(BuiltinFunctions.IS_UNKNOWN)) {
            return String.format(" %s IS UNKNOWN ", callExpr.getExprList().get(0).accept(this, arg));

        } else {
            String functionName = functionIdentifier.getName().toUpperCase().replaceAll("-", "_");
            return String.format(" %s ( %s ) ", functionName, callExpr.getExprList().stream()
                    .map(this::visitAndSwallowException).collect(Collectors.joining(", ")));
        }
    }

    @Override
    public String visit(LiteralExpr literalExpr, Void arg) {
        switch (literalExpr.getValue().getLiteralType()) {
            case STRING:
                return "\"" + literalExpr.getValue().getStringValue() + "\"";

            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case LONG:
                return literalExpr.getValue().getStringValue();

            case MISSING:
            case NULL:
            case TRUE:
            case FALSE:
                return literalExpr.getValue().getStringValue().toUpperCase();
        }
        return null;
    }

    @Override
    public String visit(VariableExpr variableExpr, Void arg) {
        return "`" + formatIdentifierForQuery(variableExpr.getVar().getValue()) + "`";
    }

    @Override
    public String visit(ListConstructor listConstructor, Void arg) {
        StringBuilder sb = new StringBuilder();
        if (listConstructor.getType() == ListConstructor.Type.ORDERED_LIST_CONSTRUCTOR) {
            sb.append(" [ ");
            sb.append(listConstructor.getExprList().stream().map(this::visitAndSwallowException)
                    .collect(Collectors.joining(", ")));
            sb.append(" ] ");

        } else {
            sb.append(" {{ ");
            sb.append(listConstructor.getExprList().stream().map(this::visitAndSwallowException)
                    .collect(Collectors.joining(", ")));
            sb.append(" }} ");
        }
        return sb.toString();
    }

    @Override
    public String visit(RecordConstructor recordConstructor, Void arg) {
        String recordConstructorString = recordConstructor.getFbList().stream()
                .map(f -> formatIdentifierForQuery(visitAndSwallowException(f.getLeftExpr())) + " : "
                        + visitAndSwallowException(f.getRightExpr()))
                .collect(Collectors.joining(", "));
        return String.format(" { %s } ", recordConstructorString);
    }

    @Override
    public String visit(OperatorExpr operatorExpr, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(operatorExpr.getExprList().get(0).accept(this, arg));
        for (int i = 0; i < operatorExpr.getOpList().size(); i++) {
            OperatorType operatorType = operatorExpr.getOpList().get(i);
            switch (operatorType) {
                case OR:
                    sb.append(" OR ");
                    break;
                case AND:
                    sb.append(" AND ");
                    break;
                case LT:
                    sb.append(" < ");
                    break;
                case GT:
                    sb.append(" > ");
                    break;
                case LE:
                    sb.append(" <= ");
                    break;
                case GE:
                    sb.append(" >= ");
                    break;
                case EQ:
                    sb.append(" = ");
                    break;
                case NEQ:
                    sb.append(" != ");
                    break;
                case PLUS:
                    sb.append(" + ");
                    break;
                case MINUS:
                    sb.append(" - ");
                    break;
                case CONCAT:
                    sb.append(" || ");
                    break;
                case MUL:
                    sb.append(" * ");
                    break;
                case DIVIDE:
                    sb.append(" / ");
                    break;
                case DIV:
                    sb.append(" DIV ");
                    break;
                case MOD:
                    sb.append(" MOD ");
                    break;
                case CARET:
                    sb.append(" ^ ");
                    break;
                case LIKE:
                    sb.append(" LIKE ");
                    break;
                case NOT_LIKE:
                    sb.append(" NOT LIKE ");
                    break;
                case IN:
                    sb.append(" IN ");
                    break;
                case NOT_IN:
                    sb.append(" NOT IN ");
                    break;
                case BETWEEN:
                    sb.append(" BETWEEN ");
                    break;
                case NOT_BETWEEN:
                    sb.append(" NOT BETWEEN ");
                    break;
                case DISTINCT:
                    sb.append(" DISTINCT ");
                    break;
                case NOT_DISTINCT:
                    sb.append(" NOT DISTINCT ");
                    break;
                default:
                    throw new IllegalStateException("Illegal operator encountered!");
            }
            sb.append(operatorExpr.getExprList().get(i + 1).accept(this, arg));
        }
        return sb.toString();
    }

    @Override
    public String visit(FieldAccessor fieldAccessor, Void arg) throws CompilationException {
        return fieldAccessor.getExpr().accept(this, arg) + ".`"
                + formatIdentifierForQuery(fieldAccessor.getIdent().getValue()) + "`";
    }

    @Override
    public String visit(IndexAccessor indexAccessor, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(indexAccessor.getExpr().accept(this, arg));
        sb.append(" [ ");
        switch (indexAccessor.getIndexKind()) {
            case ELEMENT:
                sb.append(indexAccessor.getIndexExpr().accept(this, arg));
                break;
            case STAR:
                sb.append(" * ");
                break;
            default:
                throw new NotImplementedException("Cannot handle index accessor of ANY kind.");
        }
        sb.append(" ] ");
        return sb.toString();
    }

    @Override
    public String visit(ListSliceExpression listSliceExpression, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(listSliceExpression.getStartIndexExpression().accept(this, arg));
        sb.append(" : ");
        if (listSliceExpression.hasEndExpression()) {
            sb.append(listSliceExpression.getEndIndexExpression().accept(this, arg));
        }
        return sb.toString();
    }

    @Override
    public String visit(UnaryExpr unaryExpr, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        if (unaryExpr.getExprType() != null) {
            switch (unaryExpr.getExprType()) {
                case POSITIVE:
                    sb.append("+");
                case NEGATIVE:
                    sb.append("-");
                    break;
                case EXISTS:
                    sb.append("EXISTS");
                    break;
                case NOT_EXISTS:
                    sb.append("NOT EXISTS");
                    break;
            }
        }
        sb.append(unaryExpr.getExpr().accept(this, arg));
        return sb.toString();
    }

    @Override
    public String visit(IfExpr ifExpr, Void arg) {
        throw new NotImplementedException("IfExpr are not available in SQL++!");
    }

    @Override
    public String visit(QuantifiedExpression quantifiedExpression, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(" ( ");
        if (quantifiedExpression.getQuantifier() == QuantifiedExpression.Quantifier.SOME) {
            sb.append(" SOME ");

        } else if (quantifiedExpression.getQuantifier() == QuantifiedExpression.Quantifier.SOME_AND_EVERY) {
            sb.append(" SOME AND EVERY ");

        } else {
            sb.append(" EVERY ");
        }
        sb.append(quantifiedExpression.getQuantifiedList().stream()
                .map(q -> visitAndSwallowException(q.getVarExpr()) + " IN " + visitAndSwallowException(q.getExpr()))
                .collect(Collectors.joining(", ")));
        sb.append(" SATISFIES ");
        sb.append(quantifiedExpression.getSatisfiesExpr().accept(this, arg));
        sb.append(" END ) ");
        return sb.toString();
    }

    @Override
    public String visit(CaseExpression caseExpression, Void arg) throws CompilationException {
        StringBuilder sb = new StringBuilder();
        sb.append(" CASE ");
        for (int i = 0; i < caseExpression.getWhenExprs().size(); i++) {
            Expression whenExpr = caseExpression.getWhenExprs().get(i);
            Expression thenExpr = caseExpression.getThenExprs().get(i);
            sb.append(" WHEN ");
            sb.append(whenExpr.accept(this, arg));
            sb.append(" THEN ");
            sb.append(thenExpr.accept(this, arg));
        }
        if (caseExpression.getElseExpr() != null) {
            sb.append(" ELSE ");
            sb.append(caseExpression.getElseExpr().accept(this, arg));
        }
        return sb.toString();
    }

    @Override
    public String visit(WindowExpression windowExpression, Void arg) {
        // TODO: Add support for WINDOW-EXPR here.
        throw new NotImplementedException("WindowExpression not currently supported!");
    }

    // Little helper method to dispatch within streams.
    private String visitAndSwallowException(ILangExpression e) {
        try {
            return e.accept(this, null);

        } catch (CompilationException ex) {
            throw new IllegalStateException("We should never reach here!");
        }
    }

    private String formatIdentifierForQuery(String identifier) {
        if (identifier.contains("$")) {
            return identifier.replace("$", "");

        } else if (identifier.contains("#GG")) { // This is a Graphix generated identifier.
            return identifier.replace("#GG_", "GGV_");

        } else if (identifier.contains("#")) { // This is a SQL++ generated identifier.
            return identifier.replace("#", "SGV_");

        } else {
            return identifier;
        }
    }
}
