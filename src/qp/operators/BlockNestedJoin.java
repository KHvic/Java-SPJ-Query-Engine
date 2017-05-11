/** Block nested join algorithm **/

package qp.operators;

import qp.optimizer.*;

import qp.utils.*;
import java.io.*;

public class BlockNestedJoin extends Join {

	// number of batch per out join
	int batchsize;

	// index of join attribute of left table
	int leftindex;
	// index of right attribute of right table
	int rightindex;
	// The file name where the right table is materialize
	String rfname;
	// To get unique filenum for this operation
	static int filenum = 0;

	// Output buffer
	Batch outbatch;
	// Buffer for left input stream
	Block leftBlock;
	Batch leftbatch;
	// Buffer for right input stream
	Batch rightbatch;
	// File pointer to the right hand materialized file
	ObjectInputStream in;

	// Cursor for left side buffer
	int lcurs;
	// Cursor for right side buffer
	int rcurs;
	// Whether end of stream (left table) is reached
	boolean eosl;
	// End of stream (right table)
	boolean eosr;

	// Constructor
	public BlockNestedJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	/**
	 * During open finds the index of the join attributes Materializes the right
	 * hand side into a file Opens the connections
	 **/
	public boolean open() {

		// Number of buffer Available = total buffer count - 1 input - 1 output
		int bufferNum = BufferManager.getBuffersPerJoin() - 2;
		leftBlock = new Block(bufferNum);

		/** select number of tuples per batch **/
		int tuplesize = schema.getTupleSize();
		batchsize = Batch.getPageSize() / tuplesize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);
		Batch rightpage;

		/** initialize the cursors of input buffers **/
		lcurs = 0;
		rcurs = 0;
		eosl = false;
		/**
		 * because right stream is to be repetitively scanned if it reached end,
		 * we have to start new scan
		 **/
		eosr = true;
		/**
		 * Right hand side table is to be materialized for the Nested join to
		 * perform
		 **/

		if (!right.open()) {
			return false;
		} else {
			/**
			 * If the right operator is not a base table then Materialize the
			 * intermediate result from right into a file
			 **/

			filenum++;
			rfname = "BNJtemp-" + String.valueOf(filenum);
			try {
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));

				while ((rightpage = right.next()) != null) {
					out.writeObject(rightpage);
				}
				out.close();
			} catch (IOException io) {
				System.out.println("BlockNestedJoin:writing the temporary file error");
				return false;
			}
			if (!right.close())
				return false;
		}
		if (left.open())
			return true;
		else
			return false;
	}

	/**
	 * from input buffers selects the tuples satisfying join condition And
	 * returns a page of output tuples
	 **/
	public Batch next(){

		int i,j;
		if(eosl){
			close();
		    return null;
		}
		outbatch = new Batch(batchsize);


		while(!outbatch.isFull()){

		    if(lcurs==0 && eosr==true){

			leftBlock.setTupleCount(0);
			
			for(int k=0;k<leftBlock.getMAX_BATCHES(); k++)
			{
				leftbatch =(Batch) left.next();

				if(leftbatch != null) 
				{
					leftBlock.add(k, leftbatch);
				
					
				}
				else 
					{ 
					break;
					}
			}
			if(leftBlock.getTupleCount()==0){
			    eosl=true;
			    return outbatch;
			}
			
			try{

			    in = new ObjectInputStream(new FileInputStream(rfname));
			    eosr=false;
			}catch(IOException io){
			    System.err.println("BlockNestedJoin:error in reading the file");
			    System.exit(1);
			}

		    }

		    while(eosr==false){

			try{
			    if(rcurs==0 && lcurs==0){
				rightbatch = (Batch) in.readObject();
			    }

			    for(i=lcurs;i<leftBlock.getTupleCount();i++) 
			   {
				for(j=rcurs;j<rightbatch.size();j++)
				{
				    Tuple lefttuple = leftBlock.tupleElementAt(i);
				    Tuple righttuple = rightbatch.elementAt(j);
				    System.out.println(i+","+j);
				    if(lefttuple.checkJoin(righttuple,leftindex,rightindex))
				   {
					Tuple outtuple = lefttuple.joinWith(righttuple);

					outbatch.add(outtuple);
					
					if(outbatch.isFull()){
					    if(i==leftBlock.getTupleCount()-1 && j==rightbatch.size()-1){//case 1
						lcurs=0;
						rcurs=0;
					    }else if(i!=leftBlock.getTupleCount()-1 && j==rightbatch.size()-1){//case 2
						lcurs = i+1;
						rcurs = 0;
					    }else if(i==leftBlock.getTupleCount()-1 && j!=rightbatch.size()-1){//case 3
						lcurs = i;
						rcurs = j+1;
					    }else{
						lcurs = i;
						rcurs =j+1;
					    }
					    return outbatch;
					}
				    }
				}
				
				rcurs =0;
			    }
			    lcurs=0;
			}catch(EOFException e){
			    try{
				in.close();
				eosr=true;
			    }catch (IOException io){
				System.out.println("BlockNestedJoin:Error in temporary file reading");
			    }
			}catch(ClassNotFoundException c){
			    System.out.println("BlockNestedJoin:Some error in deserialization ");
			    System.exit(1);
			}catch(IOException io){
			    System.out.println("BlockNestedJoin:temporary file reading error");
			    System.exit(1);
			}
		    }
		}
		return outbatch;
	    }

	public boolean close() {

		File f = new File(rfname);
		f.delete();
		return true;
	}

}
