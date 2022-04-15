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
package org.apache.asterix.graphix.lang.rewrites.lower;

import static org.apache.asterix.graphix.lang.rewrites.util.LowerRewritingUtil.buildLetClauseWithFromList;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.graphix.lang.rewrites.lower.assembly.AbstractLowerAssembly;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;
import org.apache.asterix.lang.sqlpp.clause.Projection;

/**
 * A helper class for building the {@link LetClause} that represents the final lowered representation of a query edge.
 * This is used as the node type for {@link AbstractLowerAssembly}.
 */
public class LowerSupplierNode {
    private final List<FromTerm> fromTermList;
    private final List<AbstractClause> whereClauses;
    private final List<Projection> projectionList;
    private final VariableExpr bindingVar;

    public LowerSupplierNode(List<FromTerm> fromTermList, List<AbstractClause> whereClauses,
            List<Projection> projectionList, VariableExpr bindingVar) {
        this.fromTermList = fromTermList;
        this.projectionList = projectionList;
        this.bindingVar = bindingVar;
        this.whereClauses = (whereClauses == null) ? new ArrayList<>() : whereClauses;
    }

    public List<Projection> getProjectionList() {
        return projectionList;
    }

    public VariableExpr getBindingVar() {
        return bindingVar;
    }

    public List<AbstractClause> getWhereClauses() {
        return whereClauses;
    }

    public LetClause buildLetClause() {
        return buildLetClauseWithFromList(fromTermList, whereClauses, projectionList, bindingVar);
    }
}
