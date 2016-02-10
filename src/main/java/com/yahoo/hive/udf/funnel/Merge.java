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
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@UDFType(deterministic = true)
@Description(name = "merge_funnel",
             value = "_FUNC_(funnel_column) - Merges funnels. Use with funnel UDF.",
             extended = "Example: SELECT merge_funnel(funnel)\n" +
                        "         FROM (SELECT funnel(action, timestamp, array('signup_page', 'email_signup'), \n" +
                        "                                                array('confirm_button'),\n" +
                        "                                                array('submit_button')) AS funnel\n" +
                        "               FROM table\n" +
                        "               GROUP BY user_id) t;")
public class Merge extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(Merge.class.getName());

    @Override
    public MergeEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        // Get the parameters
        TypeInfo [] parameters = info.getParameters();

        // Check number of arguments
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException("Please specify the funnel column.");
        }

        // Check that the param is a primitive type
        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0, "Only list type arguments are accepted but " + parameters[0].getTypeName() + " was passed as the first parameter.");
        }

        // Check that the list is an array of primitives
        if (((ListTypeInfo) parameters[0]).getListElementTypeInfo().getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "A long array argument should be passed, but " + parameters[0].getTypeName() + " was passed instead.");
        }

        // Check that the list is of type long
        // May want to add support for int/double/float later
        switch (((PrimitiveTypeInfo) ((ListTypeInfo) parameters[0]).getListElementTypeInfo()).getPrimitiveCategory()) {
            case LONG:
                break;
            default:
                throw new UDFArgumentTypeException(0, "A long array argument should be passed, but " + parameters[0].getTypeName() + " was passed instead.");
        }

        return new MergeEvaluator();
    }

    public static class MergeEvaluator extends GenericUDAFEvaluator {
        // List object inspector
        private ListObjectInspector listObjectInspector;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);

            // Setup the list object inspector.
            listObjectInspector = (ListObjectInspector) parameters[0];

            // Return the list of long inspector.
            return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MergeAggregateBuffer();
        }

        @Override
        public void iterate(AggregationBuffer aggregate, Object[] parameters) throws HiveException {
            Object p = parameters[0];
            if (p != null) {
                MergeAggregateBuffer funnelAggregate = (MergeAggregateBuffer) aggregate;
                List<Long> data = (List<Long>) listObjectInspector.getList(p);

                // If empty, just copy elements
                if (funnelAggregate.elements.isEmpty()) {
                    funnelAggregate.elements.addAll(data);
                } else {
                    // If the sizes don't match, throw an exception
                    if (funnelAggregate.elements.size() != data.size()) {
                        throw new UDFArgumentTypeException(0, "Funnels must be of the same size to merge!");
                    }
                    // Not empty, merge with existing list
                    for (int i = 0; i < data.size(); i++) {
                        funnelAggregate.elements.set(i, (data.get(i) + funnelAggregate.elements.get(i)));
                    }
                }
            }
        }

        @Override
        public void merge(AggregationBuffer aggregate, Object partial) throws HiveException {
            MergeAggregateBuffer funnelAggregate = (MergeAggregateBuffer) aggregate;
            List<Object> partialResult = (List<Object>) listObjectInspector.getList(partial);
            LongObjectInspector longObjectInspector = (LongObjectInspector) listObjectInspector.getListElementObjectInspector();

            // If empty, add all from partial
            if (funnelAggregate.elements.isEmpty()) {
                // Add the two lists of longs together
                for (int i = 0; i < partialResult.size(); i++) {
                    Long value = (Long) longObjectInspector.get(partialResult.get(i));
                    funnelAggregate.elements.add(value);
                }
            } else {
                // If the sizes don't match throw, an exception
                if (funnelAggregate.elements.size() != partialResult.size()) {
                    throw new UDFArgumentTypeException(0, "Funnels must be of the same size to merge!");
                }

                // Add the two lists of longs together
                for (int i = 0; i < partialResult.size(); i++) {
                    Long value = (Long) longObjectInspector.get(partialResult.get(i));
                    LOG.fatal(Long.toString(value));
                    funnelAggregate.elements.set(i, (value + funnelAggregate.elements.get(i)));
                }
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            ((MergeAggregateBuffer) buff).elements.clear();
        }

        @Override
        public Object terminate(AggregationBuffer aggregate) throws HiveException {
            MergeAggregateBuffer funnelAggregate = (MergeAggregateBuffer) aggregate;
            List<Long> ret = new ArrayList<Long>(funnelAggregate.elements.size());
            ret.addAll(funnelAggregate.elements);
            return ret;
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregate) throws HiveException {
            MergeAggregateBuffer funnelAggregate = (MergeAggregateBuffer) aggregate;
            List<Long> ret = new ArrayList<Long>(funnelAggregate.elements.size());
            ret.addAll(funnelAggregate.elements);
            return ret;
        }
    }
}
