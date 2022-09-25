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
package org.apache.asterix.graphix.lang.rewrite.canonical;

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrite.common.BranchLookupTable;
import org.apache.asterix.graphix.lang.struct.EdgeDescriptor;
import org.apache.asterix.lang.common.base.AbstractExpression;

public class CanonicalElementBranchConsumer implements ICanonicalElementConsumer {
    private final BranchLookupTable branchLookupTable;

    public CanonicalElementBranchConsumer(BranchLookupTable branchLookupTable) {
        this.branchLookupTable = branchLookupTable;
    }

    @Override
    public void accept(AbstractExpression ambiguousElement, List<? extends AbstractExpression> canonicalElements)
            throws CompilationException {
        if (ambiguousElement instanceof VertexPatternExpr) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR,
                    "Cannot evaluate an ambiguous dangling vertex using SWITCH_AND_CYCLE. Try EXPAND_AND_UNION.",
                    ambiguousElement.getSourceLocation());
        }
        EdgePatternExpr ambiguousEdgeElement = (EdgePatternExpr) ambiguousElement;
        if (ambiguousEdgeElement.getEdgeDescriptor().getPatternType() == EdgeDescriptor.PatternType.EDGE) {
            for (AbstractExpression canonicalElement : canonicalElements) {
                EdgePatternExpr canonicalEdge = (EdgePatternExpr) canonicalElement;
                branchLookupTable.putBranch(ambiguousEdgeElement, canonicalEdge);
            }

        } else { // ambiguousEdgeElement.getEdgeDescriptor().getPatternType() == EdgeDescriptor.PatternType.PATH
            for (AbstractExpression canonicalElement : canonicalElements) {
                PathPatternExpr canonicalPath = (PathPatternExpr) canonicalElement;
                branchLookupTable.putBranch(ambiguousEdgeElement, canonicalPath);
            }
        }
    }
}
