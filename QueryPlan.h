#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include <iostream>
#include <vector>
#include <algorithm>
#include <limits.h>
#include "Statistics.h"
#include "Node.h"
#include "Function.h"
#include "RelOp.h"

class QueryPlan {
public:
	QueryPlan(Statistics*stat);
	~QueryPlan();

	void PlanQuery();
	void PrintPlan(ostream& os);
	void ExecuteQuery();
	void AssignOut(char* out);
	void Concatenate(AndList*&, AndList*&);
	Schema* SumSchema(FuncOperator* finalFunction, Node* node);
	Schema* GroupBySchema(NameList* groupingAtts, FuncOperator* finalFunction, Node* node);
	Schema* SumSchema();


private:
	Statistics *stat;
	Node* start;
	vector<Node*> children;
	AndList* final;
	string output;
	FILE* file;
	string outChannel;

};



#endif /* QUERYPLAN_H_ */
