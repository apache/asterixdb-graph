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
package org.apache.asterix.graphix.extension;

import java.util.List;

import org.apache.asterix.algebra.base.ILangExtension;
import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.compiler.provider.ILangCompilationProvider;
import org.apache.asterix.graphix.algebra.compiler.provider.GraphixCompilationProvider;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class GraphixLangExtension implements ILangExtension {
    public static final ExtensionId LANG_EXTENSION_ID = new ExtensionId(GraphixLangExtension.class.getSimpleName(), 0);

    @Override
    public ExtensionId getId() {
        return LANG_EXTENSION_ID;
    }

    @Override
    public void configure(List<Pair<String, String>> args) {
    }

    @Override
    public ILangCompilationProvider getLangCompilationProvider(Language lang) {
        return (lang == Language.SQLPP) ? new GraphixCompilationProvider() : null;
    }

    @Override
    public ExtensionKind getExtensionKind() {
        return ExtensionKind.LANG;
    }
}
