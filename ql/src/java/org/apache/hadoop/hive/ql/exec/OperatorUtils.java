/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.mapred.OutputCollector;

public class OperatorUtils {

  public static <T> Set<T> findOperators(Operator<?> start, Class<T> clazz) {
    return findOperators(start, clazz, new HashSet<T>());
  }

  public static <T> T findSingleOperator(Operator<?> start, Class<T> clazz) {
    Set<T> found = findOperators(start, clazz, new HashSet<T>());
    return found.size() == 1 ? found.iterator().next() : null;
  }

  public static <T> Set<T> findOperators(Collection<Operator<?>> starts, Class<T> clazz) {
    Set<T> found = new HashSet<T>();
    for (Operator<?> start : starts) {
      findOperators(start, clazz, found);
    }
    return found;
  }

	/**
	 * Check if operator tree, in the direction specified forward/backward,
	 * contains any operator specified in the targetOPTypes.
	 * 
	 * @param start
	 *            list of operators to start checking from
	 * @param backward
	 *            direction of DAG traversal; if true implies get parent ops for
	 *            traversal otherwise children will be used
	 * @param targetOPTypes
	 *            Set of operator types to look for
	 * 
	 * @return true if any of the operator or its parent/children is of the name
	 *         specified in the targetOPTypes
	 * 
	 *         NOTE: 1. This employs breadth first search 2. By using HashSet
	 *         for "start" we avoid revisiting same operator twice. However it
	 *         doesn't prevent revisiting the same node more than once for some
	 *         complex dags.
	 */
	@SuppressWarnings("unchecked")
	public static boolean operatorExists(final HashSet<Operator> start,
			final boolean backward, final HashSet<OperatorType> targetOPTypes) {
		HashSet<Operator> nextSetOfOperators = new HashSet<Operator>();

		for (Operator op : start) {
			if (targetOPTypes.contains(op.getType())) {
				return true;
			}

			if (backward) {
				nextSetOfOperators.addAll(op.getParentOperators());
			} else {
				nextSetOfOperators.addAll(op.getChildOperators());
			}
		}

		if (!nextSetOfOperators.isEmpty()) {
			return operatorExists(nextSetOfOperators, backward, targetOPTypes);
		}

		return false;
	}

  @SuppressWarnings("unchecked")
  private static <T> Set<T> findOperators(Operator<?> start, Class<T> clazz, Set<T> found) {
    if (clazz.isInstance(start)) {
      found.add((T) start);
    }
    if (start.getChildOperators() != null) {
      for (Operator<?> child : start.getChildOperators()) {
        findOperators(child, clazz, found);
      }
    }
    return found;
  }

  public static void setChildrenCollector(List<Operator<? extends OperatorDesc>> childOperators, OutputCollector out) {
    if (childOperators == null) {
      return;
    }
    for (Operator<? extends OperatorDesc> op : childOperators) {
      if(op.getName().equals(ReduceSinkOperator.getOperatorName())) { //TODO:
        ((ReduceSinkOperator)op).setOutputCollector(out);
      } else {
        setChildrenCollector(op.getChildOperators(), out);
      }
    }
  }
}
