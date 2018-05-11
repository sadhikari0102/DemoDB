#include "QueryPlan.h"


extern FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern TableList *tables; // the list of tables and aliases in the query
extern AndList *boolean; // the predicate in the WHERE clause
extern NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query


QueryPlan::QueryPlan(Statistics* stat) {
	this->stat = stat;
	start=NULL;
	final=NULL;
	outChannel = "STDOUT";
}

QueryPlan::~QueryPlan() {

}

void QueryPlan::PlanQuery() {
	//cout<<"\nPlanning Query!!";
	// creating possible leaf nodes
	TableList* tempTable = tables;
	while(tempTable!=NULL) {
		//cout<<temp->tableName<<"-"<<temp->aliasAs;
		AndList* final;
		stat->CopyRel(tempTable->tableName, tempTable->aliasAs);
		//cout<<"\n"<<tempTable->tableName<<" "<<tempTable->aliasAs;
		TableNode* node = new TableNode(boolean, final, tempTable->tableName, tempTable->aliasAs, stat);
		Concatenate(this->final, final);
		children.push_back(node);
		tempTable = tempTable->next;
	}
	//cout<<"\nLeaves created!!";

	//create Join nodes
	vector<Node*> all(children);
	sort(all.begin(), all.end());
	int minCost = INT_MAX;
	int count=0;
	//permute through all the possible combinations and calculate the one with minimum cost.
	do {
		vector<Node*> allTemp(all);
		//cout<<"\n"<<"Permutation"<<count++;
		AndList* tempList=NULL;
		vector<JoinNode*> temp;
		Statistics tempStat(*stat);
		//simulate the whole join for each possible permutation
		while (allTemp.size()>1) {
			Node* left = allTemp.back();
			allTemp.pop_back();
			Node* right = allTemp.back();
			allTemp.pop_back();
			AndList* final;
			JoinNode* node = new JoinNode(boolean, final, left, right, &tempStat);
			Concatenate(tempList, final);
			allTemp.push_back(node);
			temp.push_back(node);

			if(node->estimate<=0 || node->actualCost>minCost)
				break;
		}
		int cost = allTemp.back()->estimate<0?-1:allTemp.back()->actualCost;

		for(int i=0; i<temp.size(); ++i) {
			--temp[i]->pipeCount;
			free(temp[i]);
		}

		Concatenate(boolean, tempList);
		//cout<<"\ncost of permutation - "<<cost;
		//if the new minimum is found, update the children list
		if(cost>0 && cost<minCost) {
			minCost=cost;
			children=all;
		}
	}
	while(next_permutation(all.begin(), all.end()));

	//perform the actual join and update the root
	while(children.size()>1) {
		Node* left = children.back();
		children.pop_back();
		Node* right = children.back();
		children.pop_back();
		AndList* final;
		JoinNode* node = new JoinNode(boolean, final, left, right, stat);
		Concatenate(this->final, final);
		children.push_back(node);
	}
	start = children.front();
	//cout<<"\nJoin Done!!";

	//create SUM/GROUP BY/DISTINCT nodes
	if(groupingAtts!=NULL) {
		if(finalFunction!=NULL) {

			if(distinctFunc!=NULL) {
				//cout<<"\nDistinct!!";
				start = new DistinctNode(start);
			}
			//cout<<"\nGroupBy!!";
			Schema* sch = GroupBySchema(groupingAtts, finalFunction, start);
			start = new GroupByNode(groupingAtts, finalFunction, sch, start);
		}
		else {
			cout<<"\n ERROR! No Grouping Function found. ABORTING!";
			exit(0);
		}
	}
	else if(finalFunction!=NULL) {
		//cout<<"\nSum!!";
		Schema* sch = SumSchema(finalFunction, start);
		start = new SumNode(finalFunction, sch, start);
	}
	//cout<<"\nSUM/GROUP BY/DISTINCT node created";

	//create PROJECT nodes


	if (attsToSelect!=NULL) {
		if(finalFunction==NULL && groupingAtts==NULL) {
			start = new ProjectNode(attsToSelect, start);
		}
	}
	//cout<<"\nPROJECT node created";

	//create DISTINCT node
	if(distinctAtts!=NULL) {
		start = new DistinctNode(start);
	}
	//cout<<"\nDISTINCT node created";

	//create WRITE node
	start = new WriteNode(file, start);
	swap(boolean, final);
	//cout<<"\nWRITE node created";
}

void QueryPlan::ExecuteQuery() {
	if(outChannel=="STDOUT")
		file=stdout;
	else if(outChannel==" ")
		file=NULL;
	else
		file=fopen(outChannel.c_str(),"W");
	//file=stdout;
	if(file!=NULL) {
		int nodes =start->pipeCount;
		Pipe** iopipe = new Pipe*[nodes];
		RelationalOp** ops = new RelationalOp*[nodes];
		start->execute(iopipe, ops);
		for(int i=0;i<nodes;i++)
			ops[i]->WaitUntilDone();

		for(int i=0;i<nodes;i++) {
			delete iopipe[i];
			delete ops[i];
		}
		delete[] iopipe;
		delete[] ops;
		if(file!=stdout)
			fclose(file);
	}
	start->pipeCount=0;
	delete start;
	children.clear();
}

void QueryPlan::AssignOut(char* out) {
	outChannel = out;
}

Schema* QueryPlan::SumSchema(FuncOperator* finalFunction, Node* node) {
	Schema* result;
	Function temp;
	temp.GrowFromParseTree(finalFunction, *node->sch);
	Attribute attr[2][1];
	attr[0][0]= {"Sum", Int};
	attr[1][0]= {"Sum", Double};
	int t = temp.returnsInt?0:1;
	result = new Schema("", 1, attr[t]);
	return result;
}

void QueryPlan::Concatenate(AndList*& to, AndList*& from) {
	if (to==NULL) {
		swap(to, from);
		return;
	}
	AndList *left = to;
	AndList *right = to->rightAnd;
	while(right!=NULL) {
		left = right;
		right = right->rightAnd;
	}
	left->rightAnd = from;

	from = NULL;
}

void QueryPlan::PrintPlan(ostream& os) {
	cout<<"\n\n***QUERY PLAN***\n\n";
	start->print(os,0);
}

Schema* QueryPlan::GroupBySchema(NameList* groupingAtts, FuncOperator* finalFunction, Node* node) {
	Schema* result;
	Function temp;
	NameList* tempRoot = groupingAtts;
	temp.GrowFromParseTree(finalFunction, *node->sch);
	//cout<<"\nout of function parsing!!";
	Attribute finalAtts[100];
	if(node->sch->numAtts<100) {
		int count=1;
		finalAtts[0].name="SUM";
		finalAtts[0].myType=(temp.returnsInt)?Int:Double;
		while(groupingAtts!=NULL) {

			if(node->sch->Find(groupingAtts->name)>=0) {
				finalAtts[count].name=groupingAtts->name;
				finalAtts[count].myType=node->sch->FindType(groupingAtts->name);
				count++;
			}
			else {
				cout<<"\n Attribute "<<groupingAtts->name<<" not found!! ABORTING!";
				exit(0);
			}
			groupingAtts=groupingAtts->next;
		}

		result = new Schema("",count, finalAtts);
		return result;
		groupingAtts=tempRoot;
	}
	else {
		cout<<"\n Maximum limit for attributes is 100!!\n";
		exit(0);
	}
}



