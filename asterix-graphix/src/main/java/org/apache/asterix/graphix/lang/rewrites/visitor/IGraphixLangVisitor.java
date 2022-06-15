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
package org.apache.asterix.graphix.lang.rewrites.visitor;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.graphix.lang.clause.FromGraphClause;
import org.apache.asterix.graphix.lang.clause.GraphSelectBlock;
import org.apache.asterix.graphix.lang.clause.MatchClause;
import org.apache.asterix.graphix.lang.expression.EdgePatternExpr;
import org.apache.asterix.graphix.lang.expression.GraphConstructor;
import org.apache.asterix.graphix.lang.expression.PathPatternExpr;
import org.apache.asterix.graphix.lang.expression.VertexPatternExpr;
import org.apache.asterix.graphix.lang.statement.CreateGraphStatement;
import org.apache.asterix.graphix.lang.statement.DeclareGraphStatement;
import org.apache.asterix.graphix.lang.statement.GraphDropStatement;
import org.apache.asterix.graphix.lang.statement.GraphElementDeclaration;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public interface IGraphixLangVisitor<R, T> extends ILangVisitor<R, T> {
    R visit(GraphConstructor gc, T arg) throws CompilationException;

    R visit(GraphConstructor.VertexConstructor ve, T arg) throws CompilationException;

    R visit(GraphConstructor.EdgeConstructor ee, T arg) throws CompilationException;

    R visit(DeclareGraphStatement dgs, T arg) throws CompilationException;

    R visit(CreateGraphStatement cgs, T arg) throws CompilationException;

    R visit(GraphElementDeclaration gel, T arg) throws CompilationException;

    R visit(GraphDropStatement gds, T arg) throws CompilationException;

    R visit(GraphSelectBlock gsb, T arg) throws CompilationException;

    R visit(FromGraphClause fgc, T arg) throws CompilationException;

    R visit(MatchClause mc, T arg) throws CompilationException;

    R visit(EdgePatternExpr epe, T arg) throws CompilationException;

    R visit(PathPatternExpr ppe, T arg) throws CompilationException;

    R visit(VertexPatternExpr vpe, T arg) throws CompilationException;
}
