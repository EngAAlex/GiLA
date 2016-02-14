package unipg.dafne.aggregators;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import unipg.dafne.common.datastructures.FloatWritableArray;

/**
 * This aggregator is used to store different kinds of information in a map. This aggregator should be used with caution, given that a map is an
 * expensive data structure in a distributed environment. The initial value is an empty map.
 * 
 * @author Alessio Arleo
 *
 */
public abstract class ComponentAggregatorAbstract implements Aggregator<MapWritable> {

	protected MapWritable internalState;

	@Override
	public void aggregate(MapWritable in) {
		Iterator<Entry<Writable, Writable>> allEntries = in.entrySet().iterator();
		while(allEntries.hasNext()){
			Entry<Writable, Writable> current = allEntries.next();
			if(!internalState.containsKey(current.getKey()))
				internalState.put(current.getKey(), current.getValue());
			else{
				specificAggregate(current);
			}
		}
	}

	@Override
	public MapWritable createInitialValue() {
		return new MapWritable();
	}

	@Override
	public MapWritable getAggregatedValue() {
		return internalState;
	}

	@Override
	public void reset() {
		internalState.clear();
	}

	@Override
	public void setAggregatedValue(MapWritable in) {
		internalState = in;
	}

	/**
	 * This method must be overridden in order to subclass <i>ComponentAggregatorAbstract</i>. The method is used to decide
	 * whether to insert the current java.util.map.Entry<Writable, Writable> in the map or not. Note that if the key is already present the entry will be
	 * always stored in the map.
	 * 
	 * @param A java.util.map.Entry<Writable, Writable> object representing the key-value pair to insert into the map.
	 */
	protected abstract void specificAggregate(Entry<Writable, Writable> current);

	/**
	 * This aggregator stores a value for each key until another value with same key is aggregated. 
	 * This aggregator is useful for one-to-many communication from master.compute() or from a special vertex.
	 *  In case multiple vertices write to this aggregator, its behavior is non-deterministic. 
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class ComponentMapOverwriteAggregator extends ComponentAggregatorAbstract{

		@Override
		protected void specificAggregate(Entry<Writable, Writable> current) {
			internalState.put(current.getKey(), current.getValue());
		}	
	}

	/**
	 * This aggregator sums up integer values with the same key.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class ComponentIntSumAggregator extends ComponentAggregatorAbstract{

		@Override
		protected void specificAggregate(Entry<Writable, Writable> current) {
			Integer newValue = ((IntWritable)current.getValue()).get();
			Integer currentValue = ((IntWritable)internalState.get(current.getKey())).get();
			internalState.put(current.getKey(), new IntWritable(newValue+currentValue));
		}
	}
	
	/**
	 * This aggregator keeps the maximum float coordinates (float[]{x,y}) for each key.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class ComponentFloatXYMaxAggregator extends ComponentAggregatorAbstract{

		protected float[] checkEligibility(float[] mycoords,
				float[] newest) {
			float[] arrayToSave = new float[]{Math.max(mycoords[0], newest[0]),
					Math.max(mycoords[1], newest[1])};
			return arrayToSave;
		}

		@Override
		protected void specificAggregate(Entry<Writable, Writable> current) {
			float[] myData = ((FloatWritableArray)current.getValue()).get();
			float[] foreignData = ((FloatWritableArray)internalState.get(current.getKey())).get();
			internalState.put(current.getKey(), new FloatWritableArray(checkEligibility(myData, foreignData)));
		}
	}

	/**
	 * This aggregator keeps the minimum float coordinates (float[]{x,y}) for each key.
	 * 
	 * @author Alessio Arleo
	 *
	 */
	public static class ComponentFloatXYMinAggregator extends ComponentFloatXYMaxAggregator{

		@Override
		protected float[] checkEligibility(float[] mycoords, float[] newest) {
			float[] arrayToSave = new float[]{Math.min(mycoords[0], newest[0]),
					Math.min(mycoords[1], newest[1])};
			return arrayToSave;
		};	

	}

}