
package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;
import qp.optimizer.*;

public class SortMergeJoin extends Join {

	// Number of tuples per out batch
	int batchsize;

	// Index of the join attribute in left table
	int leftindex;
	// Index of the join attribute in right table
	int rightindex;
	// The file name of where the left table before sorting
	String lfname1;
	// The file name where the right table before sorting
	String rfname1;
	// The file name of where the left table after sorting
	String lfname2;
	// The file name where the right table after sorting
	String rfname2;

	static int filenum = 0; // To get unique filenum for this operation

	Batch outbatch; // Output buffer
	Batch leftbatch; // Buffer for left input stream
	Batch rightbatch; // Buffer for right input stream
	ObjectInputStream leftIn, rightIn;// left and right table input stream

	int lcurs; // Cursor for left side buffer
	int rcurs; // Cursor for right side buffer
	boolean eosl; // Whether end of stream (left table) is reached
	boolean eosr; // End of stream (right table)

	int te = 0;
	Block leftBlock, rightBlock;// block used

	// Constructor
	public SortMergeJoin(Join jn) {
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();

		// sort merge join requires at least 3 buffer
		if (BufferManager.getBuffersPerJoin() < 3) {
			System.out.println("sort merge join requires at least 3 buffer");
			// terminate program
			System.exit(1);
		}
	}

	/**
	 * During open finds the index of the join attributes Materializes the right
	 * hand side into a file Opens the connections
	 **/
	public boolean open() {

		/** select number of tuples per batch **/
		batchsize = Batch.getPageSize() / schema.getTupleSize();

		Attribute leftattr = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);

		/** initialize the cursors of input buffers **/
		lcurs = -99;
		rcurs = -99;

		eosl = false;
		/**
		 * because right stream is to be repetitively scanned if it reached end,
		 * we have to start new scan
		 **/
		eosr = false;
		/**
		 * Right hand side table is to be materialized for the Nested join to
		 * perform
		 **/
		leftBlock = new Block(1);
		rightBlock = new Block(BufferManager.getBuffersPerJoin() - 2);

		if (!left.open() || !right.open()) {
			return false;
		} else {
			// get pre-sorted relation
			lfname1 = getRelation(left);
			rfname1 = getRelation(right);
			// get sorted relation into 2 files
			lfname2 = getSortedRelation(lfname1, left, leftattr);
			System.out.println("left sorted");
			rfname2 = getSortedRelation(rfname1, right, rightattr);
			System.out.println("right sorted");
		}
		if (lfname2 == null || rfname2 == null) {
			/* error with sorting file */
			System.out.println("SortedMergeJoin:sorting the temporary file error");
			return false;
		} else {
			try {
				leftIn = new ObjectInputStream(new FileInputStream(lfname2));
				rightIn = new ObjectInputStream(new FileInputStream(rfname2));
			} catch (Exception e) {
				System.out.println("SortedMergeJoin:writing the temporary file error");
				return false;
			}
			return true;
		}
	}

	// sort the input relation based on attribute, return null if error
	public String getRelation(Operator op) {
		if (!op.open()) {
			/* Error opening file */
			return null;
		} else {
			/* read relation in */
			filenum++;
			String preSortedRelation = "preSortedRelation-" + String.valueOf(filenum);
			try {
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(preSortedRelation));

				Batch tempPage = new Batch(batchsize);

				/** get all contains of the table, and store them in a file */
				while ((tempPage = op.next()) != null)
					out.writeObject(tempPage);

				out.close();
				return preSortedRelation;
			} catch (Exception e) {
				/* Error writing preSortedFile */
				System.out.println("Empty relation");
				return null;
			}
		}
	}

	// get a sorted relation using external sort
	public String getSortedRelation(String filename, Operator op, Attribute attr) {
		Vector<Attribute> attrVector = new Vector<Attribute>();
		attrVector.add(attr);
		try {
			// Perform external sort on preSortedRelation using Attribute
			return new ExternalSort(filename, attrVector, op.getSchema()).getSortFile();
		} catch (Exception e) {
			/* Error writing sortedFile */
			return null;
		}
	}

	/**
	 * from input buffers selects the tuples satisfying join condition And
	 * returns a page of output tuples
	 **/
	public Batch next() {
		// if either cursor not pointing to any tuples and at end of a relation
		if ((eosl && lcurs == -99) || (eosr && rcurs == -99)) {
			// can just return since has finished with reading 1 relation, won't
			// have any matches
			close();
			return null;
		}

		outbatch = new Batch(batchsize);
		// while output buffer not full
		while (!outbatch.isFull()) {
			System.out.println("running");
			if ((eosl && lcurs == -99) || (eosr && rcurs == -99))
				break;

			// read in new left block if needed
			if (lcurs == -99) {
				System.out.println("reading left block");
				leftBlock.empty();
				lcurs = 0;
				for (int i = 0; i < leftBlock.getMAX_BATCHES(); i++) {
					try {
						leftBlock.add(i, (Batch) leftIn.readObject());
					} catch (java.io.EOFException f) {
						eosl = true;
						break;
					} catch (Exception e) {
						System.err.println("SortMergeJoin:error in reading the left file");
					}
				}
			}
			// read in new right block if needed
			if (rcurs == -99) {
				System.out.println("reading right block");
				rightBlock.empty();
				rcurs = 0;
				for (int i = 0; i < rightBlock.getMAX_BATCHES(); i++) {
					try {
						rightBlock.add(i, (Batch) rightIn.readObject());
					} catch (java.io.EOFException f) {
						eosr = true;
						break;
					} catch (Exception e) {
						System.err.println("SortMergeJoin:error in reading the right file");
					}
				}
			}
			// merge sort
			do {
				System.out.println("merge sort");
				Tuple leftTuple = leftBlock.tupleElementAt(lcurs);
				Tuple rightTuple = rightBlock.tupleElementAt(rcurs);
				// if tuple matches
				if(leftTuple == null || rightTuple == null){
					if(leftTuple==null){
						eosl = true;
						lcurs = -99;
					}
					if(rightTuple==null){
						rcurs = -99;
						eosr = true;
					}
					break;
					}
				System.out.println("comparing "+ lcurs+","+rcurs);
				if (Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex) == 0) {
					Tuple outtuple = leftTuple.joinWith(rightTuple);
					// add to output page
					outbatch.add(outtuple);

					// if we have went through the entire block
					if (rcurs == (rightBlock.getTupleCount() - 1))
						rcurs = -99;
					else // have not went through entire right block
						rcurs++;
					// can return page if already full
					if (outbatch.isFull())
						return outbatch;
				}
				// left relation has less value, increment it
				else if (Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex) < 0) {
					if (lcurs == (leftBlock.getTupleCount() - 1))
						lcurs = -99;
					else
						lcurs++;
				}
				// right relation has left value, increment it
				else {
					if (rcurs == (rightBlock.getTupleCount() - 1))
						rcurs = -99;
					else
						rcurs++;
				}
			} while (lcurs != -99 && rcurs != -99);
		}

		return outbatch;
	}

	/** Close the operator */
	public boolean close() {
		try {
			leftIn.close();
			rightIn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		File f = new File(lfname1);
		f.delete();
		f = new File(rfname1);
		f.delete();
		f = new File(lfname2);
		f.delete();
		f = new File(rfname2);
		f.delete();

		return true;

	}

}