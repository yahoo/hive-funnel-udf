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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Assert;
import org.junit.Test;

public class FunnelTest {
    @Test(expected = UDFArgumentLengthException.class)
    public void testInvalidNumberOfParams() throws HiveException {
        Funnel udaf = new Funnel();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaLongObjectInspector
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testComplexParamPosition1() throws HiveException {
        Funnel udaf = new Funnel();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testComplexParamPosition2() throws HiveException {
        Funnel udaf = new Funnel();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector),
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testNonmatchingParamPosition4() throws HiveException {
        Funnel udaf = new Funnel();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaLongObjectInspector
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testComplexParamPosition4() throws HiveException {
        Funnel udaf = new Funnel();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testComplexValueInFunnelList() throws HiveException {
        Funnel udaf = new Funnel();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)),
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test(expected = UDFArgumentTypeException.class)
    public void testFunnelListValueNotMatchingWithActionColumn() throws HiveException {
        Funnel udaf = new Funnel();
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector)
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);
    }

    @Test
    public void testComplete() throws HiveException {
        Funnel udaf = new Funnel();

        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, // action_column
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,   // timestamp_column
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector), // funnel_step_1
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector) // funnel_step_1
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        ObjectInspector outputObjectInspector = udafEvaluator.init(Mode.COMPLETE, inputObjectInspectorList);

        // Order will be "alpha, beta, gamma, delta" when ordered on timestamp_column
        // Funnel is "beta" -> "gamma" -> "epsilon"
        // Should return [1, 1, 0] as we don't have an epsilon
        Object[] parameters1 = new Object[]{ "beta", 200L, new ArrayList<Object>(), Arrays.asList("beta"),                     null,   Arrays.asList("gamma"), Arrays.asList("epsilon")}; // Test empty list funnel step, and null in funnel step
        Object[] parameters2 = new Object[]{"alpha", 100L,   Arrays.asList("beta"), Arrays.asList("gamma"), Arrays.asList("epsilon")};
        Object[] parameters3 = new Object[]{"delta", 400L,   Arrays.asList("beta"), Arrays.asList("gamma"), Arrays.asList("epsilon")};
        Object[] parameters4 = new Object[]{"gamma", 200L,   Arrays.asList("beta"), Arrays.asList("gamma"), Arrays.asList("epsilon")}; // gamma and beta happen at the same time, beta should come first (sorted on action after timestamp)
        Object[] parameters5 = new Object[]{   null, 800L,   Arrays.asList("beta"), Arrays.asList("gamma"), Arrays.asList("epsilon")}; // Check null action_column
        Object[] parameters6 = new Object[]{"omega", null,   Arrays.asList("beta"), Arrays.asList("gamma"), Arrays.asList("epsilon")}; // Check null timestamp

        // Process the data
        AggregationBuffer agg = udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.iterate(agg, parameters1);
        udafEvaluator.iterate(agg, parameters2);
        udafEvaluator.iterate(agg, parameters3);
        udafEvaluator.iterate(agg, parameters4);
        udafEvaluator.iterate(agg, parameters5);
        udafEvaluator.iterate(agg, parameters6);
        Object result = udafEvaluator.terminate(agg);

        // Expected
        List<Long> expected = new ArrayList<>();
        expected.add(1L);
        expected.add(1L);
        expected.add(0L);

        Assert.assertEquals(expected, result);
    }

    @Test
    public void testPartial1() throws HiveException {
        Funnel udaf = new Funnel();

        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, // action_column
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,   // timestamp_column
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector), // funnel_step_1
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector) // funnel_step_1
        };

        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        ObjectInspector outputObjectInspector = udafEvaluator.init(Mode.PARTIAL1, inputObjectInspectorList);

        // Order will be "alpha, beta, gamma, delta" when ordered on timestamp_column
        // Funnel is "beta" -> "gamma" -> "epsilon"
        // Should return [1, 1, 0] as we don't have an epsilon
        Object[] parameters1 = new Object[]{ "beta", 200L, Arrays.asList("beta"), Arrays.asList("gamma"), Arrays.asList("epsilon")};
        Object[] parameters2 = new Object[]{"alpha", 100L, Arrays.asList("beta"), Arrays.asList("gamma"), Arrays.asList("epsilon")};
        Object[] parameters3 = new Object[]{"delta", 400L, Arrays.asList("beta"), Arrays.asList("gamma"), Arrays.asList("epsilon")};
        Object[] parameters4 = new Object[]{"gamma", 300L, Arrays.asList("beta"), Arrays.asList("gamma"), Arrays.asList("epsilon")};

        // Process the data
        AggregationBuffer agg = udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.iterate(agg, parameters1);
        udafEvaluator.iterate(agg, parameters2);
        udafEvaluator.iterate(agg, parameters3);
        udafEvaluator.iterate(agg, parameters4);
        Object result = udafEvaluator.terminatePartial(agg);

        // Expected partial output
        List<Object> expected = new ArrayList<>();
        expected.add(Arrays.asList("beta", "gamma"));
        expected.add(Arrays.asList(200L, 300L));
        expected.add(Arrays.asList("beta", null, "gamma", null, "epsilon", null));

        Assert.assertEquals(expected, result);
    }

    @Test
    public void testPartial2() throws HiveException {
        Funnel udaf = new Funnel();

        // Construct the object inspector for udaf evaluator
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, // action_column
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,   // timestamp_column
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector), // funnel_step_1
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector) // funnel_step_1
        };
        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        // Construct the struct object inspector
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("action");
        fieldNames.add("timestamp");
        fieldNames.add("funnel");
        List<ObjectInspector> fieldInspectors = new ArrayList<>();
        fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
        fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector));
        fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
        ObjectInspector structObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
        ObjectInspector[] evaluatorInputObjectInspectorList = new ObjectInspector[]{structObjectInspector};

        ObjectInspector outputObjectInspector = udafEvaluator.init(Mode.PARTIAL2, evaluatorInputObjectInspectorList);

        // Create the two structs to merge
        List<Object> parameter1 = new ArrayList<>();
        parameter1.add(Arrays.asList("beta"));
        parameter1.add(Arrays.asList(300L));
        parameter1.add(Arrays.asList("alpha", null, "beta", null, "gamma", null, "epsilon", null));

        List<Object> parameter2 = new ArrayList<>();
        parameter2.add(Arrays.asList("gamma", "alpha"));
        parameter2.add(Arrays.asList(400L, 200L));
        parameter1.add(Arrays.asList("alpha", null, "beta", null, "gamma", null, "epsilon", null));

        // Process the data
        AggregationBuffer agg = udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.merge(agg, parameter1);
        udafEvaluator.merge(agg, parameter2);
        Object result = udafEvaluator.terminatePartial(agg);

        // Expected
        List<Object> expected = new ArrayList<>();
        expected.add(Arrays.asList("beta", "gamma", "alpha"));
        expected.add(Arrays.asList(300L, 400L, 200L));
        expected.add(Arrays.asList("alpha", null, "beta", null, "gamma", null, "epsilon", null));

        Assert.assertEquals(expected, result);
    }

    @Test
    public void testFinal() throws HiveException {
        Funnel udaf = new Funnel();

        // Construct the object inspector for udaf evaluator
        ObjectInspector[] inputObjectInspectorList = new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, // action_column
            PrimitiveObjectInspectorFactory.javaLongObjectInspector,   // timestamp_column
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector), // funnel_step_1
            ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector) // funnel_step_1
        };
        GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(inputObjectInspectorList, false, false);
        GenericUDAFEvaluator udafEvaluator = udaf.getEvaluator(paramInfo);

        // Construct the struct object inspector
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("action");
        fieldNames.add("timestamp");
        fieldNames.add("funnel");
        List<ObjectInspector> fieldInspectors = new ArrayList<>();
        fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
        fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaLongObjectInspector));
        fieldInspectors.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
        ObjectInspector structObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
        ObjectInspector[] evaluatorInputObjectInspectorList = new ObjectInspector[]{structObjectInspector};

        ObjectInspector outputObjectInspector = udafEvaluator.init(Mode.FINAL, evaluatorInputObjectInspectorList);

        // Create the two structs to merge
        List<Object> parameter1 = new ArrayList<>();
        parameter1.add(Arrays.asList("beta"));
        parameter1.add(Arrays.asList(300L));
        parameter1.add(Arrays.asList("alpha", null, "beta", null, "gamma", null, "epsilon", null));

        List<Object> parameter2 = new ArrayList<>();
        parameter2.add(Arrays.asList("gamma", "alpha"));
        parameter2.add(Arrays.asList(400L, 200L));
        parameter2.add(Arrays.asList("alpha", null, "beta", null, "gamma", null, "epsilon", null));

        // Process the data
        AggregationBuffer agg = udafEvaluator.getNewAggregationBuffer();
        udafEvaluator.reset(agg);
        udafEvaluator.merge(agg, parameter1);
        udafEvaluator.merge(agg, parameter2);
        Object result = udafEvaluator.terminate(agg);

        // Expected
        List<Long> expected = Arrays.asList(1L, 1L, 1L, 0L);

        Assert.assertEquals(expected, result);
    }
}
