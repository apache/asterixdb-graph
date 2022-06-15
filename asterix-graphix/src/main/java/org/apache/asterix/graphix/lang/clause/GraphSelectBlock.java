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

import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.rewrites.visitor.IGraphixLangVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.clause.GroupbyClause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.clause.SelectBlock;
import org.apache.asterix.lang.sqlpp.clause.SelectClause;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

/**
 * Starting AST node for a Graphix query, which will replace the FROM-CLAUSE with a {@link FromGraphClause} on
 * parse. The goal of our Graphix rewriter is to replace these {@link FromGraphClause} nodes with applicable
 * {@link org.apache.asterix.lang.sqlpp.clause.FromClause} nodes.
 */
public class GraphSelectBlock extends SelectBlock {
    private FromGraphClause fromGraphClause;

    public GraphSelectBlock(SelectClause selectClause, FromGraphClause fromGraphClause,
            List<AbstractClause> letWhereClauses, GroupbyClause groupbyClause,
            List<AbstractClause> letHavingClausesAfterGby) {
        super(selectClause, null, letWhereClauses, groupbyClause, letHavingClausesAfterGby);
        this.fromGraphClause = fromGraphClause;
    }

    public FromGraphClause getFromGraphClause() {
        return fromGraphClause;
    }

    public void setFromGraphClause(FromGraphClause fromGraphClause) {
        this.fromGraphClause = fromGraphClause;
    }

    public boolean hasFromGraphClause() {
        return fromGraphClause != null;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        if (hasFromClause()) {
            return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);

        } else {
            return ((IGraphixLangVisitor<R, T>) visitor).visit(this, arg);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFromClause(), getFromGraphClause(), getGroupbyClause(), getLetWhereList(),
                getLetHavingListAfterGroupby(), getSelectClause());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof GraphSelectBlock)) {
            return false;
        }
        GraphSelectBlock target = (GraphSelectBlock) object;
        return super.equals(target) && Objects.equals(getFromGraphClause(), target.getFromGraphClause());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getSelectClause());
        if (hasFromClause()) {
            sb.append(' ').append(getFromClause());
        } else if (hasFromGraphClause()) {
            sb.append(' ').append(getFromGraphClause());
        }
        if (hasLetWhereClauses()) {
            sb.append(' ').append(getLetWhereList());
        }
        if (hasGroupbyClause()) {
            sb.append(' ').append(getGroupbyClause());
        }
        if (hasLetHavingClausesAfterGroupby()) {
            sb.append(' ').append(getLetHavingListAfterGroupby());
        }
        return sb.toString();
    }
}
