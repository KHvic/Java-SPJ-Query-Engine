package qp.operators;

import java.io.*;

import qp.utils.*;
import qp.optimizer.*;
import java.util.*;

public class ExternalSort {
	int numTuplePerPage;
	// vector to store list of index of attribute for sorting order
	Vector<Integer> attIndex = new Vector<Integer>();
	// keep track of files we have used
	Vector<String> outputFile = new Vector<String>();
	// To get unique filenum for this operation
	static int filenum = 0;
	// schema containing information required for join such as tuple count
	Schema schema;
	// filename to be sorted
	String filename;
	int B;// num buffer
	// Constructor

	public ExternalSort(String inputFile, Vector<Attribute> attributeList, Schema s) {
		schema = s;
		// read memory file
		filename = inputFile;
		// store index of attribute list
		for (int i = 0; i < attributeList.size(); i++)
			attIndex.add(schema.indexOf(attributeList.get(i)));
		// num tuple a page can hold = num byte per page / num byte per tuple
		numTuplePerPage = Batch.getPageSize() / schema.getTupleSize();
		if (BufferManager.getBuffersPerJoin() != 0)
			B = BufferManager.getBuffersPerJoin();
		else
			B = 50;

	}

	// perform external sort and materialize the file, return sorted file name
	public String getSortFile() throws IOException, ClassNotFoundException {
		// create sorted runs
		Vector<String> phase1 = phase1();
		// perform merging of sorted runs iteratively
		String sortedRun = phase2(phase1);
		clearTempFiles();
		System.out.println("external sort ended");
		
		return sortedRun;
	}

	public void clearTempFiles() {
		for (String fn : outputFile) {
			File f = new File(fn);
			f.delete();
		}
	}

	// phase 1 create sorted runs
	public Vector<String> phase1() throws IOException {
		// temporary block to perform operation
		Block memory;
		// temporary output page
		Batch outputPage = new Batch(numTuplePerPage);
		// end of the memory file
		boolean eof = false;
		// file name to write output to
		String writeFN;
		// keep track of materialized sorted run
		Vector<String> sortedRunsFN = new Vector<String>();
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename));
		ObjectOutputStream out = null;
		int min = 0;
		while (!eof) {
			// B buffer for phase 1, a memory block simulate as our memory
			memory = new Block(B);
			// new file name to be materialized
			filenum++;
			writeFN = "ExternalSortP1-" + String.valueOf(filenum) + ".data";
			outputFile.add(writeFN);
			// read as many records as possible into memory
			int i = 0;
			while (i < memory.getMAX_BATCHES()) {
				try {
					memory.add(i, (Batch) in.readObject());
					i++;
				} catch (EOFException e) {
					// no more records to read
					eof = true;
					in.close();
					break;
				} catch (Exception e) {
					System.out.println("ExternalSort:error in reading unsorted file");
					System.exit(1);
				}
			}

			// if memory contain records
			if (memory.getBatches().size() > 0) {
				// perform in-memory sort for each page
				for (i = 0; i < memory.getBatches().size(); i++)
					memory.elementAt(i).sortTuple(attIndex.firstElement());

				// produce a single B page sorted run which is materialized in a
				// file
				try {
					out = new ObjectOutputStream(new FileOutputStream(writeFN));

					// while memory is not empty
					while (memory.getTupleCount() > 0) {
						// get the index of page that contains the smallest
						// value
						min = memory.getSmallestBatch(attIndex.firstElement());
						// add the first element of page with minimum value to
						// output page
						outputPage.add(memory.elementAt(min).elementAt(0));

						// remove tuple from page
						memory.elementAt(min).remove(0);
						memory.setTupleCount(memory.getTupleCount() - 1);

						// output page is full
						if (outputPage.isFull()) {
							// write out the sorted record to our temporary file
							out.writeObject(outputPage);
							// new output page
							outputPage = new Batch(numTuplePerPage);
						}
					}
					// write remaining tuples if any
					if (outputPage.size() > 0)
						out.writeObject(outputPage);

					sortedRunsFN.add(writeFN);
					out.close();
				} catch (Exception e) {
					System.out.println("Error Producing Sorted run in Phase 1");
				}
			} else // memory is empty initially, don't do anything
				break;
		}
		return sortedRunsFN;
	}

	// phase 2 merging of sorted runs
	public String phase2(Vector<String> toReadFN) throws IOException {
		// temporary block to perform operation
		Block memory;
		// temporary output page
		Batch outputPage = new Batch(numTuplePerPage);
		// file name to write output to
		String writeFN;
		// keep track of file that have been written
		Vector<String> writtenFN = new Vector<String>();
		// multiple input stream required to read multiple files at once
		Vector<ObjectInputStream> inVector;
		// output Stream
		ObjectOutputStream out = null;
		// DEBUG if(true)return toReadFN.get(0);
		// iteratively merging until no file left to be read and only 1 file is
		// produced
		while (toReadFN.size() > 0) {
			// memory of B-1 buffer pages
			memory = new Block(B - 1);
			inVector = new Vector<ObjectInputStream>();
			// new file name to be materialized
			filenum++;
			writeFN = "ExternalSortP2-" + String.valueOf(filenum) + ".data";
			outputFile.add(writeFN);
			try {
				out = new ObjectOutputStream(new FileOutputStream(writeFN));
			} catch (Exception e) {
				System.out.println("ExternalSort: Phase2 error with output stream");
			}

			// read B-1 buffer pages from B-1 sorted runs into memory
			// input stream for each of the files to be read
			int i = 0;
			while (i < memory.getMAX_BATCHES() && !(toReadFN.size()==0)) {
				
				try {
					System.out.println(toReadFN.elementAt(0));
					if(toReadFN.isEmpty()){
						System.out.println("empty");
						break;}
					ObjectInputStream in = new ObjectInputStream(new FileInputStream(toReadFN.elementAt(0)));
					toReadFN.remove(0);
					inVector.add(in);
					// add one page of records in memory first
					memory.add(i, (Batch) in.readObject());
					i++;
				}catch (ArrayIndexOutOfBoundsException e) {
					break;
				} catch (Exception e) {
					System.out.println("ExternalSort: Phase2 error with input stream ");
					if(toReadFN.isEmpty())
						System.out.println("empty");
					for(String s : toReadFN)
						System.out.println(s);
	
				}
			}

			// while memory is not empty
			while (memory.getTupleCount() > 0) {
				// get index of page with smallest value
				int min = memory.getSmallestBatch(attIndex.firstElement());
				System.out.println(memory.getBatches().size());
				//System.out.println("min page: "+min);
				// add tuple to output page
				outputPage.add(memory.elementAt(min).elementAt(0));
				// remove tuple
				memory.elementAt(min).remove(0);
				memory.setTupleCount(memory.getTupleCount() - 1);
				

				// after removing of tuple, if page becomes empty, remove it
				if (memory.elementAt(min).size() == 0) {
					memory.remove(min);
					try {
						// read new page from file
						Batch newBatch = (Batch) inVector.elementAt(min).readObject();
						memory.add(min, newBatch);
					} catch (EOFException e1) {
						// file is empty already!
						try {
							inVector.elementAt(min).close();
							inVector.remove(min);
						} catch (Exception e2) {
							System.out.println("ExternalSort: Phase2 error with closing input stream");
						}
					} catch (Exception e) {
						System.out.println("ExternalSort: Phase2 error with reading input stream");
					}
				}

				if (outputPage.isFull()) {
					out.writeObject(outputPage);
					outputPage = new Batch(numTuplePerPage);
				}
			}
			System.out.println("memory emptied");
			// write remaining tuple to output page if any
			if (outputPage.size() > 0) {
				out.writeObject(outputPage);
				outputPage = new Batch(numTuplePerPage);
			}

			// add the file to the vector of writtenFN
			writtenFN.add(writeFN);
			out.close();
			// swap written to read for next iteration if there are still unread
			// files
			if (toReadFN.size() > 0 || writtenFN.size() != 1) {
				toReadFN = writtenFN;
				writtenFN = new Vector<String>();
			} else {// nothing left to read and only 1 sorted run produced
				outputFile.remove(writeFN);
				for (ObjectInputStream in : inVector)
					in.close();
				break;
			}
		}
		// there should only be one final file left
		if (writtenFN.size() == 1)
			return writtenFN.get(0);
		else
			return null;
	}

}
