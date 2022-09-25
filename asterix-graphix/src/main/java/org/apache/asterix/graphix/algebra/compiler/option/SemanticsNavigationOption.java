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
package org.apache.asterix.graphix.algebra.compiler.option;

import java.util.Locale;

public enum SemanticsNavigationOption implements IGraphixCompilerOption {
    NO_REPEAT_VERTICES,
    NO_REPEAT_EDGES,
    NO_REPEAT_ANYTHING;

    public static final String OPTION_KEY_NAME = "graphix.semantics.navigation";
    public static final SemanticsNavigationOption OPTION_DEFAULT = NO_REPEAT_ANYTHING;

    @Override
    public String getOptionValue() {
        return name().toLowerCase(Locale.ROOT).replace("_", "-");
    }

    @Override
    public String getDisplayName() {
        return String.format("%s ( %s )", OPTION_KEY_NAME, getOptionValue());
    }
}
