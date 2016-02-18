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

import java.util.List;
import java.util.stream.Collectors;
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
                        "                                                'confirm_button',\n" +
                        "                                                'submit_button') AS funnel\n" +
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

        // Check if the parameter is not a list
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
        /** Input list object inspector. Used during iterate. */
        private ListObjectInspector listObjectInspector;

        /** Long object inspector. Used during merge. */
        private LongObjectInspector longObjectInspector;

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);

            // Setup the list and element object inspectors.
            listObjectInspector = (ListObjectInspector) parameters[0];
            longObjectInspector = (LongObjectInspector) listObjectInspector.getListElementObjectInspector();

            // Will return a list of longs
            return ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MergeAggregateBuffer();
        }

        @Override
        public void iterate(AggregationBuffer aggregate, Object[] parameters) throws HiveException {
            Object parameter = parameters[0];
            // If not null
            if (parameter != null) {
                // Get the funnel aggregate and the funnel data
                MergeAggregateBuffer funnelAggregate = (MergeAggregateBuffer) aggregate;
                List<Long> funnel = (List<Long>) listObjectInspector.getList(parameter);

                // Add the funnel to the funnel aggregate
                funnelAggregate.addFunnel(funnel);
            }
        }

        @Override
        public void merge(AggregationBuffer aggregate, Object partial) throws HiveException {
            if (partial != null) {

                // Get the funnel aggregate and the funnel data
                MergeAggregateBuffer funnelAggregate = (MergeAggregateBuffer) aggregate;

                // Convert the partial results into a list of longs
                List<Long> funnel = listObjectInspector.getList(partial)
                                                       .stream()
                                                       .map(longObjectInspector::get)
                                                       .collect(Collectors.toList());

                // Add the funnel to the funnel aggregate
                funnelAggregate.addFunnel(funnel);
            }
        }

        @Override
        public void reset(AggregationBuffer aggregate) throws HiveException {
            MergeAggregateBuffer funnelAggregate = (MergeAggregateBuffer) aggregate;
            funnelAggregate.clear();
        }

        @Override
        public Object terminate(AggregationBuffer aggregate) throws HiveException {
            MergeAggregateBuffer funnelAggregate = (MergeAggregateBuffer) aggregate;
            return funnelAggregate.output();
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregate) throws HiveException {
            MergeAggregateBuffer funnelAggregate = (MergeAggregateBuffer) aggregate;
            return funnelAggregate.output();
        }
    }
}
