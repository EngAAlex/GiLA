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
/**
 * 
 */
package unipg.gila.common.datastructures.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class is used to carry the coordinates of the generating vertex across the graph as an array of floats.
 * 
 * @author Alessio Arleo
 *
 */
public class LayoutMessage extends MessageWritable<Long, float[]> {

	private int deg = -1;
	
	/**
	 * Parameter-less constructor
	 * 
	 */
	public LayoutMessage() {
		super();
	}
	
	/**
	 * Creates a new PlainMessage with ttl 0.
	 * 
	 * @param payloadVertex
	 * @param coords
	 */
	public LayoutMessage(long payloadVertex, float[] coords){
		super(payloadVertex, coords);		
	}
	
	/**
	 * Creates a new PlainMessage with the given ttl.
	 * 
	 * @param payloadVertex
	 * @param ttl
	 * @param coords
	 */
	public LayoutMessage(long payloadVertex, int ttl, float[] coords){
		super(payloadVertex, ttl, coords);		
	}
	
	/**
	 * Creates a new PlainMessage with the given ttl and the given deg.
	 * 
	 * @param payloadVertex
	 * @param ttl
	 * @param coords
	 */
	public LayoutMessage(long payloadVertex, int ttl, float[] coords, int deg){
		this(payloadVertex, ttl, coords);
		setDeg(deg);
	}
	
	public void setDeg(int deg){
		this.deg = deg;
	}

	public int getDeg(){
		return deg;
	}
	
	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagate()
	 */
	@Override
	public MessageWritable<Long, float[]> propagate() {
		LayoutMessage toReturn = new LayoutMessage(payloadVertex, ttl-1, new float[]{value[0], value[1]}, deg);
		return toReturn;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#propagateAndDie()
	 */
	@Override
	public MessageWritable<Long, float[]> propagateAndDie() {
		LayoutMessage toReturn = new LayoutMessage(payloadVertex, 0, new float[]{value[0], value[1]}, deg);
		return toReturn;
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificRead(java.io.DataInput)
	 */
	@Override
	protected void specificRead(DataInput in) throws IOException {
		payloadVertex = in.readLong();
		value = new float[2];
		value[0] = in.readFloat();
		value[1] = in.readFloat();
		deg = in.readInt();			
	}

	/* (non-Javadoc)
	 * @see unipg.dafne.common.datastructures.messagetypes.MessageWritable#specificWrite(java.io.DataOutput)
	 */
	@Override
	protected void specificWrite(DataOutput out) throws IOException {
		out.writeLong(payloadVertex);
		out.writeFloat(value[0]);
		out.writeFloat(value[1]);
		out.writeInt(deg);
	}

}
