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
package unipg.gila.layout;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import unipg.gila.common.coordinatewritables.CoordinateWritable;
import unipg.gila.common.datastructures.PartitionedLongWritable;
import unipg.gila.common.datastructures.messagetypes.LayoutMessage;
import unipg.gila.layout.GraphReintegration.FairShareReintegrateOneEdges;
import unipg.gila.layout.force.FR;
import unipg.gila.layout.force.Force;
import unipg.gila.utils.Toolbox;

/**
 * The propagator class works as follows. For each message received:
 * 
 * 1) If the message is received from a vertex not analyzed yet then the acting force on it is computed.
 * 	  If the vertex who sent the message is a neighbor then the attractive forces are computed; otherwise
 * 	  only the repulsive ones.
 * 
 * 2) If the message TTL is > 0 it is decreased and the message is broadcasted to the vertex neighbors.
 * 
 * 3) If the messages queues are activated, a portion of the messages is popped from the queue and broadcasted.
 * 
 * @author Alessio Arleo
 *
 */
public class Propagator extends AbstractComputation<PartitionedLongWritable, 
CoordinateWritable, NullWritable, LayoutMessage, LayoutMessage>{

	//LOGGER
	Logger log = Logger.getLogger(Propagator.class);
	
	protected float minimumForceThreshold;
	protected Float k;
	protected float walshawConstant;	
	protected Force force;

	@SuppressWarnings("unchecked")
	@Override
	public void initialize(
			GraphState graphState,
			WorkerClientRequestProcessor<PartitionedLongWritable, CoordinateWritable, NullWritable> workerClientRequestProcessor,
			GraphTaskManager<PartitionedLongWritable, CoordinateWritable, NullWritable> graphTaskManager,
			WorkerGlobalCommUsage workerGlobalCommUsage,
			WorkerContext workerContext) {
		super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
				workerGlobalCommUsage, workerContext);

		k = ((FloatWritable)getAggregatedValue(FloodingMaster.k_agg)).get();
		walshawConstant = ((FloatWritable)getAggregatedValue(FloodingMaster.walshawConstant_agg)).get();

		try {
			force = ((Class<Force>)Class.forName(getConf().get(FloodingMaster.forceMethodOptionString, FR.class.toString()))
						).newInstance();
		} catch (Exception e) {
			force = new FR();
		}
		force.generateForce(getConf().getStrings(FloodingMaster.forceMethodOptionExtraOptionsString, ""), k);

	}

	@Override
	public void compute(
			Vertex<PartitionedLongWritable, CoordinateWritable, NullWritable> vertex,
			Iterable<LayoutMessage> messages)
					throws IOException {

		Iterator<LayoutMessage> it = messages.iterator();
		CoordinateWritable vValue = vertex.getValue();

		float[] mycoords = vValue.getCoordinates();;	
		float[] foreigncoords;

		float[] finalForce = new float[]{0.0f, 0.0f};
		float[] repulsiveForce = new float[]{0.0f, 0.0f};
		
		int v1Deg;
		int v2Deg;
		
		while(it.hasNext()){	
			LayoutMessage currentMessage = it.next();

			LongWritable currentPayload = new LongWritable(currentMessage.getPayloadVertex());

			if(currentMessage.getPayloadVertex().equals(vertex.getId().getId()) || vValue.isAnalyzed(currentPayload))
				continue;
			
			foreigncoords=currentMessage.getValue();

			float squareDistance = Toolbox.squareModule(mycoords, foreigncoords);
			float distance = new Float(Math.sqrt(squareDistance));
			
			float deltaX = foreigncoords[0] - mycoords[0];
			float deltaY = foreigncoords[1] - mycoords[1];		
			
			v1Deg = vertex.getNumEdges() + vValue.getOneDegreeVerticesQuantity();
			v2Deg = currentMessage.getDeg();
									
			//ATTRACTIVE FORCES
			if(vValue.hasBeenReset()){
				float[] tempForce = force.computeAttractiveForce(deltaX, deltaY, distance, squareDistance, v1Deg, v2Deg);				
				finalForce[0] += tempForce[0];
				finalForce[1] += tempForce[1];
			}

			//REPULSIVE FORCES
			float [] tempForce = force.computeRepulsiveForce(deltaX, deltaY, distance, squareDistance, v1Deg, v2Deg);

			repulsiveForce[0] += tempForce[0];
			repulsiveForce[1] += tempForce[1];
			
			vValue.analyze(currentPayload);

			if(!currentMessage.isAZombie()){
				aggregate(FloodingMaster.MessagesAggregatorString, new BooleanWritable(false));
				sendMessageToAllEdges(vertex, (LayoutMessage) currentMessage.propagate());					
			}

		}

		//REPULSIVE FORCE MODERATION
		repulsiveForce[0] *= walshawConstant;
		repulsiveForce[1] *= walshawConstant;

		finalForce[0] -= repulsiveForce[0];
		finalForce[1] -= repulsiveForce[1];

		vValue.setAsMoving();
		vValue.addToForceVector(finalForce);

	}

}
