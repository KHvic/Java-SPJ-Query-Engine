/** sort merge join algorithm **/

package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;
import  qp.optimizer.*;


public class SortMergeJoin extends Join{

	//Number of tuples per out batch
	int batchsize;  

	 // Index of the join attribute in left table
    int leftindex;    
 // Index of the join attribute in right table
    int rightindex; 
  //The file name of where the left table before sorting
    String lfname1;	
 // The file name where the right table before sorting
    String rfname1;    
  //The file name of where the left table after sorting
String lfname2;
//The file name where the right table after sorting
String rfname2;
    
    String rftempname; // for intermedium result of right table
    static int filenum=0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs; // Cursor for right side buffer
    int oldrcurs;//Cursor for the first tuple in right side block which many tuples with the same value
    int rightTempCounter;// how many block that stored in the rightTemp file
    int leftInput;
    int rightInput;
    boolean eosl;  // Whether end of stream (left table) is reached
    boolean eosr;  // End of stream (right table)
    boolean eosrTemp;
    boolean readRightTemp;//the state that read block from temp file of right table
    boolean writeRightTemp;//the state that write block to the temp file of right table
    boolean startReadTemp;//the state that read the first block from the temp file, we need to set the rcurs to oldrcurs
    //boolean felr;	//Whether the first equal last in the block of right table,if yes, the right table need to be stored in disk
    
    Tuple preTuple;// the previous tuple in left table
    
    int numLeftBuff, numRightBuff;//buffer number used by left and right table
    Block leftBlock, rightBlock;//block used
    
    ObjectInputStream leftois, rightois;//used for reading sorted file, batch object 
    ObjectInputStream rightTempois;
    ObjectOutputStream rightTempoos;
    
    //Constructor
    public SortMergeJoin(Join jn){
	super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
	schema = jn.getSchema();
	jointype = jn.getJoinType();
	numBuff = BufferManager.getBuffersPerJoin();
	numLeftBuff=1;//usually the buffer only need 1
	numRightBuff=BufferManager.getBuffersPerJoin()-2;// debug numBuff-2;// the rest buffer used for right table
	
	//sort merge join requires at least 3 buffer
		if(BufferManager.getBuffersPerJoin()<3) {
			System.out.println("sort merge join requires at least 3 buffer");
			//terminate program
			System.exit(1);
		}
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/
    public boolean open(){

		/** select number of tuples per batch **/
		int tuplesize=schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr =(Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);
		
		/** initialize the cursors of input buffers **/
		lcurs = -1; rcurs =-1; 
		oldrcurs=-1;//old right cursor -1:no record old cursor
		eosl=false;
		/** because right stream is to be repetitively scanned
		 ** if it reached end, we have to start new scan
		 **/
		eosr=false;
		/** Right hand side table is to be materialized
		 ** for the Nested join to perform
		 **/
		leftBlock=new Block(numLeftBuff);
		rightBlock=new Block(numRightBuff);
		
		eosrTemp=true;
		readRightTemp=false;
		writeRightTemp=false;
		startReadTemp=false;
		rightTempCounter=0;
		leftInput=0;
		rightInput=0;
		
	

		rightTempois=null;
		rightTempoos=null;
		
		
		if(!left.open() || !right.open()) {
			return false;
		}
		else {
			//get pre sorted relation
			lfname1=getRelation(left);			
			rfname1=getRelation(right);
			//get sorted relation into 2 files
			lfname2=getSortedRelation(lfname1,left,leftattr);
			rfname2=getSortedRelation(rfname1,left,leftattr);
		}
		if(lfname2==null || rfname2==null) {
			/*error with sorting file*/
			System.out.println("SortedMergeJoin:sorting the temporary file error");
			return false;
		}
		else {//come back here!!
			try {
				leftois=new ObjectInputStream(new FileInputStream(lfname2));
				rightois=new ObjectInputStream(new FileInputStream(rfname2));
			}
			catch (Exception e) {
				System.out.println("SortedMergeJoin:writing the temporary file error");
				return false;
			}
			return true;
		}
    }
    
    //sort the input relation based on attribute, return null if error
    public String getRelation(Operator op) {
    	if(!op.open()) {
    		/*Error opening file*/
    		return null;
    	}
    	else {
    		/*read relation in*/
    		filenum++;
    		String preSortedRelation="preSortedRelation-"+String.valueOf(filenum);
    		try {
    			ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream(preSortedRelation));
    			
    			int tupleSize=op.getSchema().getTupleSize();
    			int batchSize=Batch.getPageSize()/tupleSize;
    			
    			Batch tempPage=new Batch(batchSize);
    			
    			/**get all contains of the table, and store them in a file*/
    			while((tempPage=op.next())!=null) {
    				out.writeObject(tempPage);
    			//	tempOutputBatch=new Batch(batchSize);
    			}
    			
    			out.close();
    			return preSortedRelation;
    		}
    		catch (Exception e) {
    			/*Error writing preSortedFile*/
    			return null;
    		}
    	}
    }

    public String getSortedRelation(String filename,Operator op,Attribute attr){
    	Vector<Attribute> attrVector=new Vector<Attribute>();
		attrVector.add(attr);
		//Perform external sort on preSortedRelation using Attribute
		ExternalSort es=new ExternalSort(filename,attrVector,op.getSchema());
		try {
			return es.getSortFile();
		}
		catch (Exception e) {
			/*Error writing sortedFile*/
			return null;
		}
    }

	

    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/

    
    //do NOT consider duplicate
    public Batch next() {
    	if((eosl && lcurs==-1) || (eosr && rcurs==-1)) {
    		close();
    		return null;
    	}
    	outbatch=new Batch(batchsize);
    	while(!outbatch.isFull()) {
    		if((eosl && lcurs==-1) || (eosr && rcurs==-1)) {
    			break;
    		}
    		
    		if(lcurs==-1) {
    			leftBlock.clear();
    			for(int i=0;i<numLeftBuff;i++) {
    	    		try {
    					if (eosl) break;

    	    				leftBlock.add(i,(Batch)leftois.readObject());
    	    			//}
    	    		}
    				catch(java.io.EOFException f)
    				{
    					eosl=true;				    		
    				}
    	    		catch(Exception e){
    	    			e.printStackTrace();	    				
    	    		}	
    			}
    			if(leftBlock.getTupleCount()>0) {
    				lcurs++;
    			}
    		}
    		
    		if(rcurs==-1) {
    			rightBlock.clear();
 				for (int i = 0; i < numRightBuff; i++) {
					try {
						if (eosr)
							break;
						rightBlock.add(i, (Batch) rightois.readObject());
						//	}	
						
					} catch (java.io.EOFException f) {
						eosr = true;
					} catch (Exception e) {
						e.printStackTrace();
					}
    			}
 				if(rightBlock.getTupleCount()>0) {
 					rcurs++;
 				}
    		}
    		
    		while(lcurs!=-1 && rcurs!=-1) {		
    			Tuple leftTuple=leftBlock.tupleElementAt(lcurs);
    			Tuple rightTuple=rightBlock.tupleElementAt(rcurs);

				if (Tuple.compareTuples(leftTuple, rightTuple, leftindex,rightindex)==0) {
					Tuple outpuTuple=leftTuple.joinWith(rightTuple);
					outbatch.add(outpuTuple);	
					if(rcurs==(rightBlock.getTupleCount()-1)) {
						rcurs=-1;
					}
					else {
						rcurs++;
					}
				}
				//result <0
				else if(Tuple.compareTuples(leftTuple, rightTuple, leftindex,rightindex)<0){
					if(lcurs==(leftBlock.getTupleCount()-1)) {
						lcurs=-1;
					}
					else {
						lcurs++;				
					}
				}
				//result >0
				else{
					if(rcurs==(rightBlock.getTupleCount()-1)) {
						rcurs=-1;
					}
					else {
						rcurs++;
					}				
				}	

    			
    			
	    		if(outbatch.isFull()) {
	    			return outbatch;
	    		}    			
    		}
    	}
    	if(outbatch.size()>0) {
    		return outbatch;	
    	}
    	else {
			return null;
		}
    	
    }

    /** Close the operator */
    public boolean close(){
    	System.out.println(lfname1);
    	System.out.println(lfname2);
   File f = new File(lfname1);
   f.delete();
f = new File(rfname1);
f.delete();
f= new File(lfname2);
f.delete();
f= new File(rfname2);
    f.delete();
	return true;

    }


}