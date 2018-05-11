#ifndef SORTEDDBFILE_H_
#define SORTEDDBFILE_H_

#include <string>

#include "DBFile.h"
#include "Pipe.h"
#include "BigQ.h"

class SortedDBFile: public GenericDBFile {
//class SortedDBFile {
friend class DBFile;
private:
	Pipe *input;
	Pipe *output;
	BigQ *bigQ;
	OrderMaker ordermaker;
	int runlen;


public:
	SortedDBFile() {}

	~SortedDBFile() {}

	int Create(const char *f_path, fType f_type, void *startup);
	int Open (const char *f_path, string fname);
	void Add (Record &addme);
	void MoveFirst();
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	int Close ();


private:
	void MergeData();
	int BinarySearch(Record& fetchme, Record& literal, ComparisonEngine& compare, OrderMaker& query, OrderMaker& cnforder);
	void CustomOrderMaker(OrderMaker &final, OrderMaker &sort, OrderMaker &cnforder, CNF &cnf);
	SortedDBFile(const SortedDBFile&);
	SortedDBFile& operator=(const SortedDBFile&);
};


#endif /* SORTEDDBFILE_H_ */
