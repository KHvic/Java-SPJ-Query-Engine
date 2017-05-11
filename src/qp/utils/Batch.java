/** Batch represents a page **/

package qp.utils;

import java.util.Vector;
import java.io.Serializable;

public class Batch implements Serializable{

    int MAX_SIZE;  // Number of tuples per page
    static int PageSize;  /* Number of bytes per page**/

    Vector<Tuple> tuples; // The tuples in the page


	/** Set number of bytes per page **/
	public static void setPageSize(int size){
		PageSize=size;
	}

	/** get number of bytes per page **/
	public static int getPageSize(){
		return PageSize;
	}

	/** Number of tuples per page **/

    public Batch(int numtuple){
		MAX_SIZE=numtuple;
	tuples = new Vector<Tuple>(MAX_SIZE);
    }


    /** insert the record in page at next free location**/

    public void add(Tuple t){
	tuples.add(t);
    }
  	
    public int capacity(){
	return MAX_SIZE;

    }


    public void clear(){
	tuples.clear();
    }

    public boolean contains(Tuple t){
	return tuples.contains(t);
    }

    public Tuple elementAt(int i){
	return (Tuple) tuples.elementAt(i);
    }

    public int indexOf(Tuple t){
	return tuples.indexOf(t);
    }

    public void insertElementAt(Tuple t, int i){
	tuples.insertElementAt(t,i);
    }

    public boolean isEmpty(){
	return tuples.isEmpty();
    }

    public void remove(int i){
	tuples.remove(i);
    }

    public void setElementAt(Tuple t, int i){
	tuples.setElementAt(t,i);
    }

    public int size(){
	return tuples.size();
    }

    public boolean isFull(){
	if(size() == capacity())
	    return true;
	else
	    return false;
    }
    //sort the page
  	public void sortTuple(int i) {
  		int low=0;
  		//iterative sort
  		for(int k=0; k<size()-1;k++) {
  			for(int j=k+1;j<size();j++) 
  				//compare j with min, if j is smaller
  				if(Tuple.compareTuples((Tuple)tuples.get(low), (Tuple)tuples.get(j),i)>0) 
  					low=j;
  			//if min changed
  			if(low!=k) 
  				swapTuple(k,low);
  			low=k+1;
  		}
  	}
  	//swap 2 tuples
  	public void swapTuple(int i, int j){
  			Tuple tempTuple=(Tuple) tuples.remove(i);
			tuples.insertElementAt(tuples.elementAt(j-1),i);
			tuples.remove(j);
			tuples.insertElementAt(tempTuple, j);
  	}
}













