/*
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.hive.udf.funnel;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@UDFType(deterministic = true)
@Description(name = "fallout",
             value = "fallout(funnel) - Converts a funnel from raw counts to fallout rates.",
             extended = "Converts absolute count funnel to fallout rates.")
public class Fallout extends GenericUDF {
    static final Log LOG = LogFactory.getLog(Fallout.class.getName());

    private ListObjectInspector listInputObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("The operator 'fallout' accepts 1 argument.");
        }

        // Check that the argument is a list type
        if (arguments[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(1, "Only list type arguments are accepted, but " + arguments[0].getTypeName() + " was passed.");
        }

        // Check that the list is of type long
        // May want to add support for int/double/float later
        switch (((PrimitiveObjectInspector) ((ListObjectInspector) arguments[0]).getListElementObjectInspector()).getPrimitiveCategory()) {
            case LONG:
                break;
            default:
                throw new UDFArgumentTypeException(1, "A long array argument should be passed, but " + arguments[0].getTypeName() + " was passed instead.");
        }

        // Get the list object inspector
        listInputObjectInspector = (ListObjectInspector) arguments[0];

        // This UDF will return a list of doubles
        return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        // Check that we only have one argument
        if (args.length != 1) {
            return null;
        }

        // Get the deferred object into a list
        List<Long> funnel = (List<Long>) listInputObjectInspector.getList(args[0].get());

        // Stores our return list
        List<Double> result = new ArrayList<>();

        // If funnel is empty, return
        if (funnel.size() <= 0) {
            return result;
        }

        // First element is always 0%
        result.add(0.0);

        // Starting from the second element, calculate fallout rate
        for (int i = 1; i < funnel.size(); i++) {
            // Check for 0's
            if (funnel.get(i) <= 0 || funnel.get(i - 1) <= 0) {
                result.add(0.0);
            } else {
                // No 0's, calculate ratio
                result.add(1 - ((double) funnel.get(i) / funnel.get(i - 1)));
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Converts absolute count funnel to fallout rates.";
    }
}
