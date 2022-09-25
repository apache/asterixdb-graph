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
package org.apache.asterix.graphix.metadata.entity.schema;

import java.io.Serializable;

import org.apache.asterix.graphix.common.metadata.IElementIdentifier;
import org.apache.asterix.graphix.lang.struct.ElementLabel;

/**
 * Metadata interface for a graph element (i.e. edge or vertex). An element has the following:
 * 1. A {@link Serializable}, to uniquely identify the element across other graph elements.
 * 2. A {@link ElementLabel} unique amongst the element classes.
 * 3. A non-null SQL++ string, representing a graph element body.
 */
public interface IElement extends Serializable {
    IElementIdentifier getIdentifier();

    ElementLabel getLabel();

    String getDefinitionBody();
}
