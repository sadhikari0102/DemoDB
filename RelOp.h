#ifndef REL_OP_H
#define REL_OP_H

#include <pthread.h>

#include <iostream>
#include "Pipe.h"
#include "DBFile.h"
#include "Function.h"
#include "BigQ.h"
#include "RecordBuffer.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone ();

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;

	//common method to launch the corresponding thread
	int Launch_Thread(void*(*thread_func) (void*), void* arg);

	static void MergeRecords(Record* left, Record* right, Record* result);


	pthread_t operation;
};

class SelectFile : public RelationalOp { 

	private:
	// pthread_t thread;
	// Record *buffer;
	typedef struct {
		DBFile* dbfile;
		Pipe* pipe;
		CNF* cnf;
		Record* record;
	}opinput;
	opinput input;
	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);

	void Use_n_Pages (int n) {}
	static void* Thread_Function(void* param);

};

class SelectPipe : public RelationalOp {
	private:
	typedef struct {
		Pipe* inpipe;
		Pipe* outpipe;
		CNF* cnf;
		Record* record;
	}opinput;

	opinput input;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);

	void Use_n_Pages (int n) {}
	static void* Thread_Function(void* param);
};

class Project : public RelationalOp { 
	private:
	typedef struct {
		Pipe* inpipe;
		Pipe* outpipe;
		int* keepMe;
		int numAttsInput;
		int numAttsOutput;
	}opinput;

	opinput input;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);

	void Use_n_Pages (int n) {}
	static void* Thread_Function(void* param);
};

class Join : public RelationalOp { 
	private:
	typedef struct {
		Pipe* leftpipe;
		Pipe* rightpipe;
		Pipe* outpipe;
		CNF* selOp;
		Record* literal;
		size_t runlength;

	}opinput;
	size_t runlength;

	opinput input;
	public:
	Join();
	~Join();
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);

	void Use_n_Pages (int n);
	static void* Thread_Function(void* param);
	static int SortAndMerge(Pipe* leftpipe, Pipe* rightpipe, Pipe* outpipe, OrderMaker* left, OrderMaker* right, CNF* selOp, Record* literal, size_t runlength);
	static int BlockNestedLoop(Pipe* leftpipe, Pipe* rightpipe, Pipe* outpipe, CNF* selOp, Record* literal, size_t runlength);
	static int FileAndBuffer(RecordBuffer &buffer, DBFile& file, Pipe& pipe, Record& literal, CNF& selOp);
};

class DuplicateRemoval : public RelationalOp {
	private:
	private:
	typedef struct {
		Pipe* inpipe;
		Pipe* outpipe;
		Schema* myschema;
		size_t runlength;
	}opinput;
	size_t runlength;

	opinput input;
	public:
	DuplicateRemoval();
	~DuplicateRemoval();
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);

	void Use_n_Pages (int n);
	static void* Thread_Function(void* param);
};

class Sum : public RelationalOp {
	private:
	typedef struct {
		Pipe* inpipe;
		Pipe* outpipe;
		Function* computeMe;
	}opinput;

	opinput input;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);

	void Use_n_Pages (int n) {}
	static void* Thread_Function(void* param);
	template <class T> static int Add(Pipe* in, Pipe* out, Function* computeMe);
};

class GroupBy : public RelationalOp {

	private:
	typedef struct {
		Pipe* inpipe;
		Pipe* outpipe;
		OrderMaker* order;
		Function* computeMe;
		size_t runlength;
		//
		Schema* join_sch;

	}opinput;
	size_t runlength;


	opinput input;
	public:
	GroupBy();
	~GroupBy();
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);

	void Use_n_Pages (int n);
	static void* Thread_Function(void* param);
	template<class T> static int GroupAndOperate(Pipe* in, Pipe* out, OrderMaker* order, Function* compute, size_t runlength);
	template <class T> static void CloseGroup(Record &previous, T& sum, Pipe&out, OrderMaker&order);
};

class WriteOut : public RelationalOp {
	private:
	typedef struct {
		Pipe* inpipe;
		FILE* outfile;
		Schema* myschema;

	}opinput;

	opinput input;
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);

	void Use_n_Pages (int n) {}
	static void* Thread_Function(void* param);
};


#endif
