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

import java.util.List;

import org.apache.asterix.graphix.common.metadata.GraphElementIdentifier;
import org.apache.asterix.graphix.common.metadata.GraphIdentifier;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewritingContext;
import org.apache.asterix.graphix.lang.rewrites.common.ElementLookupTable;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.declared.MetadataProvider;

public class LowerSupplierContext {
    private final GraphixRewritingContext graphixRewritingContext;
    private final GraphIdentifier graphIdentifier;
    private final List<VertexPatternExpr> danglingVertices;
    private final List<VarIdentifier> optionalVariables;
    private final List<PathPatternExpr> pathPatternExprList;
    private final ElementLookupTable<GraphElementIdentifier> elementLookupTable;

    public LowerSupplierContext(GraphixRewritingContext graphixRewritingContext, GraphIdentifier graphIdentifier,
            List<VertexPatternExpr> danglingVertices, List<VarIdentifier> optionalVariables,
            List<PathPatternExpr> pathPatternExprList, ElementLookupTable<GraphElementIdentifier> elementLookupTable) {
        this.elementLookupTable = elementLookupTable;
        this.danglingVertices = danglingVertices;
        this.optionalVariables = optionalVariables;
        this.graphIdentifier = graphIdentifier;
        this.pathPatternExprList = pathPatternExprList;
        this.graphixRewritingContext = graphixRewritingContext;
    }

    public GraphIdentifier getGraphIdentifier() {
        return graphIdentifier;
    }

    public ElementLookupTable<GraphElementIdentifier> getElementLookupTable() {
        return elementLookupTable;
    }

    public List<VertexPatternExpr> getDanglingVertices() {
        return danglingVertices;
    }

    public List<VarIdentifier> getOptionalVariables() {
        return optionalVariables;
    }

    public MetadataProvider getMetadataProvider() {
        return graphixRewritingContext.getMetadataProvider();
    }

    public VarIdentifier getNewVariable() {
        return graphixRewritingContext.getNewVariable();
    }

    public List<PathPatternExpr> getPathPatternExprList() {
        return pathPatternExprList;
    }
}
