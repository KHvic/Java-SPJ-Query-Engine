/** Scans the base relational table **/
/*incomplete*/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.Vector;

/** Scan operator - read data from a file */

public class Distinct extends Operator {

	Operator base;
	String filename; // corresponding file name

	int batchsize; // Number of tuples per out batch;
	Batch inbatch = null; // input
	Batch outbatch = null;// output
	Vector<String> outputFile = new Vector<String>();
	ObjectInputStream in; // Input file being scanned

	boolean eos; // To indicate whether end of stream reached or not
	int cursor = 0;
	Tuple previousTuple = null;
	int numAttribute = 0;

	String unsortedFile;
	String sortedFile;

	/** Constructor - just save filename */

	public Distinct(Operator base, int type) {
		super(type);
		this.base = base;
	}

	/** Open file prepare a stream pointer to read input file */

	public boolean open() {

		/** num of tuples per batch **/
		int tuplesize = schema.getTupleSize();
		batchsize = Batch.getPageSize() / tuplesize;

		// System.out.println("Scan:----------Scanning:"+tabname);
		eos = false;
		unsortedFile = "unsortedFileDistinct.data";
		outputFile.add(unsortedFile);
		if (!base.open())
			return false;
		try {
			ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(unsortedFile));

			Batch tempbatch;
			while ((tempbatch = (Batch) base.next()) != null) {
				out.writeObject(tempbatch);
			}
			out.close();
			ExternalSort es = new ExternalSort(unsortedFile, this.schema.getAttList(), this.schema);
			sortedFile = es.getSortFile();
			outputFile.add(sortedFile);
		} catch (Exception e) {
			System.err.println(" Error reading 1 " + sortedFile);
			return false;
		}
		try {
			in = new ObjectInputStream(new FileInputStream(sortedFile));
		} catch (Exception e) {
			System.err.println(" Error reading 2 " + sortedFile);
			return false;
		}
		return true;

	}

	/**
	 * Next operator - get a tuple from the file
	 **
	 **
	 ***/

	public Batch next() {
		outbatch = new Batch(batchsize);
		/** The file reached its end and no more to read **/
		int i = 0;
		if (eos) {
			close();
			return null;
		}

		while (!outbatch.isFull()) {

			// read in new page
			if (cursor == 0) {
				try {
					inbatch = (Batch) in.readObject();
				} catch (EOFException e) {
					eos = true;
					return outbatch;
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (inbatch == null) {
					eos = true;
					return outbatch;
				}
			}

			for (i = cursor; i < inbatch.size() && (!outbatch.isFull()); i++) {
				Tuple newTuple = inbatch.elementAt(i);
				boolean same = true;

				if (previousTuple != null) {
					// check for each column of tuple
					for (int j = 0; j < newTuple._data.size(); j++)
						if (Tuple.compareTuples(previousTuple, newTuple, j) != 0)
							same = false;
				} else
					same = false;
				if (!same) {
					outbatch.add(newTuple);
					previousTuple = newTuple;
				} else {
					System.out.println("Remove duplicated tuple: ");
					Debug.PPrint(previousTuple);
				}
			}
			if (i == inbatch.size())
				cursor = 0;
			else
				cursor = i;
		}
		return outbatch;
	}

	/**
	 * Close the file.. This routine is called when the end of filed is already
	 * reached
	 **/

	public void setBase(Operator base) {
		this.base = base;
	}

	public Operator getBase() {
		return base;
	}

	public boolean close() {
		for (String fn : outputFile) {
			File f = new File(fn);
			f.delete();
		}
		try {
			in.close();
		} catch (IOException e) {
			System.err.println("Scan: Error closing " + filename);
			return false;
		}
		return true;
	}

	public Object clone() {
		Operator newbase = (Operator) base.clone();
		Distinct newscan = new Distinct(newbase, optype);
		newscan.setSchema((Schema) schema.clone());
		return newscan;
	}
}
