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
package org.apache.asterix.graphix.algebra.compiler.provider;

import java.util.List;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.compiler.provider.DefaultRuleSetFactory;
import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.rewriter.base.AbstractRuleController;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.IRuleSetKind;

public class GraphixRuleSetFactory implements IRuleSetFactory {
    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites(
            ICcApplicationContext appCtx) {
        return DefaultRuleSetFactory.buildLogical(appCtx);
    }

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getLogicalRewrites(IRuleSetKind ruleSetKind,
            ICcApplicationContext appCtx) {
        if (ruleSetKind == RuleSetKind.SAMPLING) {
            return DefaultRuleSetFactory.buildLogicalSampling();

        } else {
            throw new IllegalArgumentException(String.valueOf(ruleSetKind));
        }
    }

    @Override
    public List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> getPhysicalRewrites(
            ICcApplicationContext appCtx) {
        return DefaultRuleSetFactory.buildPhysical(appCtx);
    }
}
