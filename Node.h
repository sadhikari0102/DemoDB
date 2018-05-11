#ifndef NODE_H_
#define NODE_H_

#include <iostream>
#include <algorithm>
#include "Statistics.h"
#include "Schema.h"
#include "Function.h"
#include "RelOp.h"



class Node {
public:
	Node(const string&op, Schema* out, char* rName, char* realName, Statistics* stat);
	Node(const string&op, Schema* out, char* rName[], char* realName[], int rCount, Statistics* stat);
	Node(const string&op, Schema* out, Statistics* stat);
	~Node();

	AndList* UpdateFromSchema(Schema* sch, AndList*& alist);
	bool Contains(OrList* olist, Schema* sch);

	static int pipeCount;

	Schema* sch;
	int estimate;
	int actualCost;
	char* relNames[20];
	char* realNames[20];
	int relCount;
	Statistics* stat;
	string what;
	int pipeId;

	virtual void print(ostream& os, int level);
	virtual void printLevel(ostream& os, int level)=0;
	virtual void printPipe(ostream& os, int level)=0;
	virtual void printChildren(ostream& os, int level)=0;
	virtual void execute(Pipe** iopipe, RelationalOp** ops) = 0;

};

class DoubleSource : public Node {
public:
	DoubleSource( const string& op, Node* left, Node* right, Statistics* stat);
	~DoubleSource();
	Node *left, *right;
	int lpipe, rpipe;
	void printPipe (ostream& os, int level) ;
	void printChildren (ostream& os, int level) {
			left->print(os, level+1) ;
			right->print(os, level+1);
	}
};

class SingleSource : public Node {
public:
	SingleSource(const string& op, Node* child, Schema* sch, Statistics* stat);
	~SingleSource();
	Node *child;
	int pipe;
	void printPipe (ostream& os, int level) ;
	void printChildren (ostream& os, int level) {
		child->print(os, level+1) ;
	}
};

class JoinNode : public DoubleSource {
public:
	JoinNode(AndList*& boolean, AndList*& final, Node* left, Node* right, Statistics* stat);
	~JoinNode();

	CNF cnf;
	Record literal;
	void printLevel (ostream& os, int level);
	void execute(Pipe** iopipe, RelationalOp** ops);
};

class DistinctNode : public SingleSource {
public:
	DistinctNode(Node* node);
	~DistinctNode();

	OrderMaker* order;

	void printLevel (ostream& os, int level) {}
	void execute(Pipe** iopipe, RelationalOp** ops);
};

class SumNode : public SingleSource {
public:
	SumNode(FuncOperator* finalFunction, Schema* sch, Node* node);
	~SumNode();

	Function func;
	void printLevel (ostream& os, int level);
	void execute(Pipe** iopipe, RelationalOp** ops);
};

class GroupByNode : public SingleSource {
public:
	GroupByNode(NameList* groupingAtts, FuncOperator* finalFunction, Schema* sch, Node* node);
	~GroupByNode();

	OrderMaker order;
	Function func;
	void printLevel (ostream& os, int level);
	void execute(Pipe** iopipe, RelationalOp** ops);
};

class ProjectNode : public SingleSource {
public:
	ProjectNode(NameList* attsToSelect, Node* node);
	~ProjectNode();
	int inAtts, outAtts;
	int finalAtts[100];
	void printLevel (ostream& os, int level);
	void execute(Pipe** iopipe, RelationalOp** ops);
};

class WriteNode : public SingleSource {
public:
	WriteNode(FILE*& file, Node* node);
	~WriteNode();

	FILE*& output;
	void printLevel (ostream& os, int level);
	void execute(Pipe** iopipe, RelationalOp** ops);
};

class TableNode : public Node {
public:
	TableNode(AndList* boolean, AndList* final, char* relName, char* alias, Statistics* stat);
	~TableNode();

	bool check;
	CNF cnf;
	Record literal;
	DBFile dbfile;
	void printLevel (ostream& os, int level)  ;
	void printOperator (ostream& os, int level) ;
	void printPipe (ostream& os, int level) ;
	void printChildren (ostream& os, int level) {}
	void execute(Pipe** iopipe, RelationalOp** ops);
};



#endif /* NODE_H_ */
