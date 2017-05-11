/** Block = several Batch **/

package qp.utils;

import java.util.Vector;
import java.io.Serializable;

import qp.operators.Debug;

public class Block implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//Max number of pages the block can contain
	private int MAX_BATCHES; 
    //A block has a vector of batches
    private Vector<Batch> batches;
    
    //keep track of number of tuples in the block
    private int tupleCount;  
    //keep track of number of pages in the block
    private int batchCount;
    
    //Constructor
	public Block (int numberBatch)
		{
		MAX_BATCHES = numberBatch;
		batches = new Vector<Batch>(MAX_BATCHES);
		tupleCount = 0;
		batchCount = 0;
		}

	//remove and return a page from the block
	public Batch remove(int i) {
		tupleCount=tupleCount-batches.get(i).size();
		batchCount -=1;
		return batches.remove(i);
	}
	
	//add a page to the block
	public void add (int i, Batch page)
		{
		batches.add(i, page);
		tupleCount+=page.size();
		batchCount +=1;
		}
	
	//empty out block
	public void empty() {
		batches.clear();
		tupleCount=0;
		batchCount = 0;
	}

	//return index of page with first element that have smallest value using attribute i
	public int getSmallestBatch(int i) {
		int smallestIndex=0;
		for(int j=0;j<batches.size();j++) {
			//ignore empty page
			if(batches.elementAt(j).size()==0) 
				continue;
			else{
				try {
					//compare first element this page and the smallest index page found so far
					if(Tuple.compareTuples(batches.elementAt(smallestIndex).elementAt(0), batches.elementAt(j).elementAt(0), i)>0) //new page has smaller value
						smallestIndex=j;
				}
				catch (ArrayIndexOutOfBoundsException e) {
					smallestIndex=j;
					continue;
				}
				catch (Exception e) {
				}
			}
		}
		return smallestIndex;
	}
	
	//return page at index i
	public Batch elementAt(int i) {
		return batches.get(i);
	}
	//return number of pages
	public int getBatchCount() {
		return batches.size();
	}

	//get tuple of block given index i directly
	public Tuple tupleElementAt(int i)
		{
		Batch tempBatch;
				//out of index
		       if(i>tupleCount)  
		       	   return null;
			   else
			{
			       
			   	for(int j=0; j<batches.size(); j++) 
			   		{
			   			tempBatch = (Batch) batches.elementAt(j);
			   			if(i<tempBatch.size()) 
			   				return (Tuple) tempBatch.elementAt(i);
						else  
							i -= tempBatch.size();
			   		}
				return null;
			 }
			
		}

	
	/*Start of getter and setter*/
	public int getMAX_BATCHES() {
		return MAX_BATCHES;
	}

	public void setMAX_BATCHES(int mAX_BATCHES) {
		MAX_BATCHES = mAX_BATCHES;
	}

	public Vector<Batch> getBatches() {
		return batches;
	}

	public void setBatches(Vector<Batch> batches) {
		this.batches = batches;
	}

	public int getTupleCount() {
		return tupleCount;
	}

	public void setTupleCount(int tupleCount) {
		this.tupleCount = tupleCount;
	}
}




