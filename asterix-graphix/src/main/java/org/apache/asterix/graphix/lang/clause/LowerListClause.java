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

import org.apache.asterix.graphix.lang.clause.extension.LowerListClauseExtension;
import org.apache.asterix.graphix.lang.rewrite.lower.struct.ClauseCollection;
import org.apache.asterix.lang.common.base.AbstractExtensionClause;
import org.apache.asterix.lang.common.base.IVisitorExtension;
import org.apache.asterix.lang.common.clause.LetClause;
import org.apache.asterix.lang.sqlpp.clause.FromTerm;

/**
 * A functional equivalent to the {@link FromTerm}, used as a container for lowering a non-recursive portion of a
 * {@link FromGraphClause}. We also maintain a list of {@link LetClause} nodes that bind vertex, edge, and path
 * variables to expressions after the main linked list.
 */
public class LowerListClause extends AbstractExtensionClause {
    private final LowerListClauseExtension lowerClauseExtension;
    private final ClauseCollection clauseCollection;

    public LowerListClause(ClauseCollection clauseCollection) {
        this.clauseCollection = clauseCollection;
        this.lowerClauseExtension = new LowerListClauseExtension(this);
    }

    public ClauseCollection getClauseCollection() {
        return clauseCollection;
    }

    @Override
    public IVisitorExtension getVisitorExtension() {
        return lowerClauseExtension;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clauseCollection.hashCode());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LowerListClause)) {
            return false;
        }
        LowerListClause that = (LowerListClause) object;
        return Objects.equals(this.clauseCollection, that.clauseCollection);
    }

    @Override
    public String toString() {
        return clauseCollection.toString();
    }
}
