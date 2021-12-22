/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.graphix.algebra.compiler.provider;

import org.apache.asterix.algebra.base.ILangExpressionToPlanTranslatorFactory;
import org.apache.asterix.compiler.provider.IRuleSetFactory;
import org.apache.asterix.compiler.provider.SqlppCompilationProvider;
import org.apache.asterix.graphix.algebra.translator.GraphixExpressionToPlanTranslatorFactory;
import org.apache.asterix.graphix.lang.parser.GraphixParserFactory;
import org.apache.asterix.graphix.lang.rewrites.GraphixRewriterFactory;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.IRewriterFactory;

public class GraphixCompilationProvider extends SqlppCompilationProvider {
    @Override
    public IParserFactory getParserFactory() {
        return new GraphixParserFactory();
    }

    @Override
    public IRuleSetFactory getRuleSetFactory() {
        return new GraphixRuleSetFactory();
    }

    @Override
    public IRewriterFactory getRewriterFactory() {
        return new GraphixRewriterFactory(getParserFactory());
    }

    @Override
    public ILangExpressionToPlanTranslatorFactory getExpressionToPlanTranslatorFactory() {
        return new GraphixExpressionToPlanTranslatorFactory();
    }
}
