#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"


using namespace std;

class BigQ {
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

class Hardware{
public:
    Pipe *in;
    Pipe *out;
    OrderMaker order;
    int runLength;
};

class SortRecordComparator{
    ComparisonEngine compare;
	OrderMaker* order;
public:
	SortRecordComparator(OrderMaker* sortorder) {order=sortorder;}

	bool operator()(Record *R1,Record *R2) {
		return compare.Compare(R1,R2,order)<0;
	}
};
//a data structure to keep track of a record and the run it belongs to
class EnclosedRecord{
public:
	Record record;
	int runNum;
};

class RecordComparator{
    ComparisonEngine compare;
	OrderMaker* order;
public:
	RecordComparator(OrderMaker *sortorder){order=sortorder;}
	bool operator()(EnclosedRecord *left,EnclosedRecord *right)
	{
	    return compare.Compare(&(left->record),&(right->record),order)>=0;
	}
};



#endif
