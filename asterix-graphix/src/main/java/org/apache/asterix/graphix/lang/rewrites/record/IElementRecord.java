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
package org.apache.asterix.graphix.lang.rewrites.record;

import org.apache.asterix.lang.common.base.Expression;

public interface IElementRecord {
    // The details surrounding our element will be included in the following field (alongside our body).
    String ELEMENT_DETAIL_NAME = "_GraphixElementDetail";

    // We attach the following fields to the SELECT containing a declaration body.
    String ELEMENT_LABEL_FIELD_NAME = "Label";

    Expression getExpression();
}
