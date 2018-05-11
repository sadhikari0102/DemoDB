#ifndef HEAPDBFILE_H
#define HEAPDBFILE_H

#include "DBFile.h"

class HeapDBFile: public GenericDBFile {
//class HeapDBFile {
friend class DBFile;
public:
	HeapDBFile() {}
	~HeapDBFile() {}
	void Add (Record &addme);
	void MoveFirst();
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	int Close();
private:
	HeapDBFile(const HeapDBFile&);
	HeapDBFile& operator=(const HeapDBFile&);
};



#endif
