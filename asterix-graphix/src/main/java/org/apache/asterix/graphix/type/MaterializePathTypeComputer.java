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
package org.apache.asterix.graphix.type;

import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class MaterializePathTypeComputer extends AbstractResultTypeComputer {
    public static final String VERTICES_FIELD_NAME = "Vertices";
    public static final String EDGES_FIELD_NAME = "Edges";
    public static final ARecordType TYPE;
    static {
        String typeName = "materializedPathType";
        String[] fieldNames = new String[] { VERTICES_FIELD_NAME, EDGES_FIELD_NAME };
        AOrderedListType listType = new AOrderedListType(RecordUtil.FULLY_OPEN_RECORD_TYPE, null);
        TYPE = new ARecordType(typeName, fieldNames, new IAType[] { listType, listType }, false);
    }

    // Our type computer will always return the type above (while respecting NULL/MISSING IN -> OUT semantics).
    public static final IResultTypeComputer INSTANCE = new MaterializePathTypeComputer();

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) {
        return TYPE;
    }
}
