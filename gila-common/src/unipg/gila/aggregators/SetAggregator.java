/*******************************************************************************
 * Copyright 2016 Alessio Arleo
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
 *******************************************************************************/
package unipg.gila.aggregators;

import org.apache.giraph.aggregators.Aggregator;

import unipg.gila.common.datastructures.LongWritableSet;

public class SetAggregator implements Aggregator<LongWritableSet> {

	private LongWritableSet internalState;
	
	public void aggregate(LongWritableSet in) {
		internalState.addAll(in.get());
	}

	public LongWritableSet createInitialValue() {
		internalState = new LongWritableSet();
		return internalState;
	}

	public LongWritableSet getAggregatedValue() {
		return internalState;
	}

	public void reset() {
		internalState.reset();
	}

	public void setAggregatedValue(LongWritableSet initialSeed) {
		internalState = initialSeed;
	}

}
