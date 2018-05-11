#ifndef RECORD_H
#define RECORD_H

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#include "Defs.h"
#include "ParseTree.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"



// Basic record data structure. Data is actually stored in "bits" field. The layout of bits is as follows:
//	1) First sizeof(int) bytes: length of the record in bytes
//	2) Next sizeof(int) bytes: byte offset to the start of the first att
//	3) Byte offset to the start of the att in position numAtts
//	4) Bits encoding the record's data

class Record {

friend class ComparisonEngine;
friend class Page;

private:

	void SetBits (char *bits);
	void CopyBits(char *bits, int b_len);

public:
	char *bits;
	Record ();
	~Record();

	char* GetBits ();

	//custom constructor to create a record with single attribute
	template <class T> Record(const T& value);

	// suck the contents of the record fromMe into this; note that after
	// this call, fromMe will no longer have anything inside of it
	void Consume (Record *fromMe);

	// make a copy of the record fromMe; note that this is far more 
	// expensive (requiring a bit-by-bit copy) than Consume, which is
	// only a pointer operation
	void Copy (Record *copyMe);

	// reads the next record from a pointer to a text file; also requires
	// that the schema be given; returns a 0 if there is no data left or
	// if there is an error and returns a 1 otherwise
	int SuckNextRecord (Schema *mySchema, FILE *textFile);

	int ComposeRecord (Schema *mySchema, const char *src);

	// this projects away various attributes... 
	// the array attsToKeep should be sorted, and lists all of the attributes
	// that should still be in the record after Project is called.  numAttsNow
	// tells how many attributes are currently in the record
	void Project (const int *attsToKeep, int numAttsToKeep, int numAttsNow);

	// takes two input records and creates a new record by concatenating them;
	// this is useful for a join operation
	// attsToKeep[] = {0, 1, 2, 0, 2, 4} --gets 0,1,2 records from left 0, 2, 4 recs from right and startOfRight=3
	// startOfRight is the index position in attsToKeep for the first att from right rec
	void MergeRecords (Record *left, Record *right, int numAttsLeft, 
		int numAttsRight, int *attsToKeep, int numAttsToKeep, int startOfRight);

	// prints the contents of the record; this requires
	// that the schema also be given so that the record can be interpreted
	void Print (Schema *mySchema);

	template<class T> void Populate(T& sum);

	int GetSize() const;
    int GetPointer(size_t n) const;
    void SetPointer(size_t n, int offset);
    void Alloc(size_t len);
    template <class T> void SetValue(size_t n, const T& value);
};


//definition of custom constructor
template <class T>
Record :: Record(const T& value) {
	size_t totSpace = 2*sizeof(int)+sizeof(T); //total space needed including pointer and length
	/*if(bits)
		delete[] bits;*/
	bits = new char[totSpace]; //allocate the total space needed to the bit stream
	//data in the form of length, pointer and value
	((int*)bits)[0] = totSpace;
	((int*)bits)[1] = 2*sizeof(int);
	size_t offset = ((int*)bits)[1];
	*(T*)(bits+offset) = value;

}



//It modifies the size of the record based on the result to be added as an extra column.
//Used in GroupBy or Sum
template<class T>
void Record :: Populate(T& sum) {
	//Total size which will be needed after fitting new data
	size_t totSpace = GetSize() + sizeof(int)+sizeof(T);
	int numAtt = GetSize()<=4 ? 0 : ((int*)bits)[1]/sizeof(int)-1;

	//temporary record to hold the old data
	Record rec;
	rec.Alloc(totSpace);
	rec.SetPointer(0, sizeof(int)*(numAtt+2));
	rec.SetValue(0, sum);
	//shifting the attributes to fit in the new one
	int i=1;
	while(i<numAtt) {
		rec.SetPointer(i, GetPointer(i-1)+sizeof(int)+sizeof(T));
		i++;
	}
	size_t temp = sizeof(int)+sizeof(T);
	//Allocating and copying additional memory required
	memcpy((void*)(rec.bits+(GetPointer(0)+temp)),(void*)(bits+GetPointer(0)), GetSize()-GetPointer(0));

	//Copying final data back to original record
	Consume(&rec);
}

//sets the value of nth attribute to 'value', which can differe in type
template <class T>
void Record :: SetValue(size_t n, const T& value) {
	size_t offset = GetPointer(n);
	*(T*)(bits+offset) = value;
}

#endif
