#include "Node.h"
#include <cstdlib>

extern char* catalog_path;
extern char* dbfile_dir;
extern char* tpch_dir;

Node::Node(const string& op, Schema* out, char* rName, char* realName, Statistics* stat) {
	what=op;
	sch=out;
	relCount=0;
	estimate=0;
	actualCost=0;
	this->stat=stat;
	pipeId = pipeCount++;
	relNames[relCount]=strdup(rName);
	realNames[relCount++]=strdup(realName);

}

Node::Node(const string& op, Schema* out, char* rName[],  char* realName[], int rCount, Statistics* stat) {
	what=op;
	sch=out;
	relCount=0;
	estimate=0;
	actualCost=0;
	this->stat=stat;
	pipeId = pipeCount++;
	for(int i=0;i<rCount;i++) {
		relNames[relCount]=strdup(rName[i]);
		realNames[relCount++]=strdup(realName[i]);
	}

}

Node::Node(const string&op, Schema* out, Statistics* stat) {
	what=op;
	sch=out;
	this->stat=stat;
	relCount=0;
	estimate=0;
	actualCost=0;
	pipeId = pipeCount++;
}

Node::~Node() {

}

int Node::pipeCount=0;


//filter those attributes out which are not present in the supplied schema
AndList* Node::UpdateFromSchema(Schema* sch, AndList*& alist) {
	//sch->PrintSchema();
	AndList root, *left, *right, *final=NULL;
	root.rightAnd=alist;
	left=&root;
	right=alist;

	while(right!=NULL) {
		if(Contains(right->left, sch)) {
			left->rightAnd = right->rightAnd;
			right->rightAnd = final;
			final = right;
		}
		else
			left=right;

		right=left->rightAnd;
	}

	alist = root.rightAnd;
	return final;
}

bool Node::Contains(OrList* olist, Schema* sch) {

	while(olist!=NULL) {
		Operand *l,*r;
		l=olist->left->left;
		r=olist->left->right;

		//cout<<"\ncheck schema "<<l->value<<","<<r->value;
		if((l->code!=NAME || sch->Find(l->value)!=-1) &&
				(r->code!=NAME || sch->Find(r->value)!=-1));
			//cout<<"\nboth found";
		else {
			//cout<<"\nnot found";
			return false;
		}
		olist=olist->rightOr;
	}
	return true;

}

TableNode::TableNode(AndList* boolean, AndList* final, char* relName, char* alias, Statistics* stat):
		Node("Select", new Schema(catalog_path, relName, alias), alias, relName, stat) {
	check=false;
	final = UpdateFromSchema(sch, boolean);
	//cout<<"\nNew Statistics - \n";
	//stat->Print();
	estimate = stat->Estimate(final, relNames, relCount);
	stat->Apply(final, relNames, relCount);
	cnf.GrowFromParseTree(final, sch, literal);

}

DoubleSource::DoubleSource(const string& op, Node* left, Node* right, Statistics* stat):
	Node(op, new Schema(*left->sch, *right->sch), stat) {
	this->left=left;
	this->right=right;
	lpipe=left->pipeId;
	rpipe=right->pipeId;

	for(int i=0;i<left->relCount;i++) {
		relNames[relCount]=strdup(left->relNames[i]);
		realNames[relCount]=strdup(left->realNames[i]);
		relCount++;
	}

	for(int i=0;i<right->relCount;i++) {
		relNames[relCount]=strdup(right->relNames[i]);
		realNames[relCount]=strdup(right->realNames[i]);
		relCount++;
	}
}

DoubleSource::~DoubleSource() {

}

JoinNode::JoinNode(AndList*& boolean, AndList*& final, Node* left, Node* right, Statistics* stat):
	DoubleSource("Join", left, right, stat) {
	final = UpdateFromSchema(sch, boolean);
	//cout<<"\n"<<boolean->left->left->left->value<<","<<boolean->left->left->right->value;
	/*if(final!=NULL)
		cout<<"\n"<<final->left->left->left->value<<","<<final->left->left->right->value;

	cout<<"\n";sch->PrintSchema();*/
	estimate = stat->Estimate(final, relNames, relCount);
	stat->Apply(final, relNames, relCount);
	/*if(estimate>=0) {
		cout<<"\n Stat after joining "<<left->what<<", "<<right->what;
		//stat->Print();
	}*/
	actualCost=left->actualCost + right->actualCost + estimate;
	cnf.GrowFromParseTree(final, left->sch, right->sch, literal);
}

JoinNode::~JoinNode() {

}

SingleSource::SingleSource(const string& op, Node* child, Schema* sch, Statistics* stat):
	Node(op, sch, child->relNames, child->realNames, child->relCount, stat) {
	this->child = child;
	this->pipe = child->pipeId;
}

SingleSource::~SingleSource() {

}

DistinctNode::DistinctNode(Node* node):
	SingleSource("Distinct", node, new Schema(*node->sch), NULL) {
	order = new OrderMaker(node->sch);
}

DistinctNode::~DistinctNode() {

}

GroupByNode::GroupByNode(NameList* groupingAtts, FuncOperator* finalFunction, Schema* sch, Node* node):
	SingleSource("Group By", node, sch, NULL) {
	//sch->PrintSchema();
	while(groupingAtts!=NULL) {
		int temp = sch->Find(groupingAtts->name);
		if(temp>=0) {
			//cout<<"\norder maker "<<groupingAtts->name<<"-"<<sch->FindType(groupingAtts->name);
			order.whichAtts[order.numAtts]=temp;
			order.whichTypes[order.numAtts]=sch->FindType(groupingAtts->name);
			order.numAtts++;
		}
		else {
			cout<<"\nGiven Grouping attribute doesn't exist in the schema! ABORTING!";
			exit(0);
		}
		groupingAtts=groupingAtts->next;
	}
	func.GrowFromParseTree(finalFunction, *node->sch);
}

GroupByNode::~GroupByNode() {

}

SumNode::SumNode(FuncOperator* finalFunction, Schema* sch, Node* node):
	SingleSource("Sum", node, sch, NULL) {
	func.GrowFromParseTree(finalFunction, *node->sch);
}

SumNode::~SumNode() {

}

ProjectNode::ProjectNode(NameList* attsToSelect, Node* node):
	SingleSource("Project", node, NULL, NULL) {

	Attribute Atts[100];
	//int keep[100];
	inAtts=node->sch->GetNumAtts();
	outAtts=0;
	if(node->sch->GetNumAtts()<=99) {
		while(attsToSelect!=NULL) {
			int temp = node->sch->Find(attsToSelect->name);
			//cout<<"\n"<<attsToSelect->name;
			if(temp>=0) {
				finalAtts[outAtts]=temp;
				Atts[outAtts].name=attsToSelect->name;
				Atts[outAtts].myType=node->sch->FindType(attsToSelect->name);
			}
			else {
				cout<<"\n Attribute "<<attsToSelect->name<<" not found!! ABORTING!";
				exit(0);
			}
			outAtts++;
			attsToSelect=attsToSelect->next;
		}
	}
	else {
		cout<<"\n Maximum limit for attributes is 100!!\n";
		exit(0);
	}
	//reverse(begin(Atts), end(Atts));
	for(int i=0;i<outAtts/2;i++) {
		Type temptype = Atts[i].myType;
		char* tempname = Atts[i].name;
		Atts[i].myType=Atts[outAtts-i-1].myType;
		Atts[i].name=Atts[outAtts-i-1].name;
		Atts[outAtts-i-1].myType=temptype;
		Atts[outAtts-i-1].name=tempname;

		int temp=finalAtts[i];
		finalAtts[i]=finalAtts[outAtts-i-1];
		finalAtts[outAtts-i-1]=temp;
	}
	//reverse(begin(finalAtts), end(finalAtts));
	sch = new Schema("", outAtts, Atts);

}

ProjectNode::~ProjectNode() {

}

WriteNode::WriteNode(FILE*& file, Node* node):
	SingleSource("WriteOut", node, new Schema(*node->sch), NULL), output(file) {

}

WriteNode::~WriteNode() {

}
void Node::print(ostream& os, int level) {

	os  <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" NODE "<< what << ": " ;
	printLevel(os, level);
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level<<" Schema -";
	sch->PrintSchema(os, level);
	printPipe(os, level) ;
	os <<"\n";
	printChildren(os, level) ;

}

void SingleSource::printPipe(ostream& os, int level) {
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level  <<" "<< "Output pipe:" << pipeId;
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<<  "Input pipe:" << pipe; //check this
}

void DoubleSource::printPipe(ostream& os, int level) {
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<<  "Output pipe:" << pipeId;
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<<  "Input pipe:" << lpipe << "," << rpipe;
}

void TableNode::printPipe(ostream& os, int level) {
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<< "Output pipe:" << pipeId;
}

void TableNode::printOperator(ostream& os, int level)  {
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<<  "Select from " << relNames[0] << ": ";
}

void TableNode::printLevel(ostream& os, int level)  {
	printOperator(os, level);
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<< "CNF";
	cnf.Print(os,level);
}

void JoinNode::printLevel(ostream& os, int level)  {
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<< "CNF";
	cnf.Print(os, level);
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<<  "Estimate = " << estimate << ", Cost = " << actualCost;
}

void WriteNode::printLevel(ostream& os, int level) {
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<<  "Output to " << output;
}

void SumNode::printLevel(ostream& os, int level)  {
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<<  "Function: ";
	(const_cast<Function*>(&func))->Print(os, level); //check this
}

void GroupByNode::printLevel(ostream& os, int level)  {
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<<  "OrderMaker: ";
	(const_cast<OrderMaker*>(&order))->Print(os, level);
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<< "Function: ";
	(const_cast<Function*>(&func))->Print(os, level);
}

void ProjectNode::printLevel(ostream& os, int level)  {
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" Attribute(s)"<<  finalAtts[0];
	for (int i=1; i<outAtts; ++i)
		os << ',' << finalAtts[i];
	os <<"\n"<< (string(3*(level), ' ') + "Level ")<<level <<" "<< inAtts << " input attributes; " << outAtts << " output attributes";
}


void TableNode::execute(Pipe** iopipe, RelationalOp** ops) {
	//cout<<"\nexecuting table node";
	//cout<<"\n"<<realNames[0];
	string db = string(dbfile_dir)+string(realNames[0])+".bin";
	dbfile.Open((char*)db.c_str());
	check=true;
	SelectFile* sfile= new SelectFile();
	iopipe[pipeId] = new Pipe(PIPE_SIZE);
	ops[pipeId] = sfile;
	sfile->Run(dbfile,*iopipe[pipeId], cnf, literal);
}

void JoinNode::execute(Pipe** iopipe, RelationalOp** ops) {
	//cout<<"\nexecuting Join node";
	left -> execute(iopipe, ops);
	right -> execute(iopipe, ops);
	Join* join = new Join();
	iopipe[pipeId] = new Pipe(PIPE_SIZE);
	ops[pipeId] = join;
	join->Run(*iopipe[lpipe], *iopipe[rpipe], *iopipe[pipeId], cnf, literal);
}

void GroupByNode::execute(Pipe** iopipe, RelationalOp** ops) {
	//cout<<"\nexecuting GroupBy node";
	child -> execute(iopipe, ops);
	GroupBy* group = new GroupBy();
	iopipe[pipeId] = new Pipe(PIPE_SIZE);
	ops[pipeId] = group;
	group -> Run(*iopipe[pipe], *iopipe[pipeId], order, func);
}

void SumNode::execute(Pipe** iopipe, RelationalOp** ops) {
	//cout<<"\nexecuting Sum node";
	child->execute(iopipe, ops);
	Sum* sum = new Sum();
	iopipe[pipeId] = new Pipe(PIPE_SIZE);
	ops[pipeId] = sum;
	sum->Run(*iopipe[pipe], *iopipe[pipeId], func);

}

void DistinctNode::execute(Pipe** iopipe, RelationalOp** ops) {
	//cout<<"\nexecuting distinct node";
	child -> execute(iopipe, ops);
	DuplicateRemoval* duplicate = new DuplicateRemoval();
	iopipe[pipeId] = new Pipe(PIPE_SIZE);
	ops[pipeId] = duplicate;
	duplicate -> Run(*iopipe[pipe], *iopipe[pipeId], *sch);
}

void WriteNode::execute(Pipe** iopipe, RelationalOp** ops) {
	//cout<<"\nexecuting write node";
	child->execute(iopipe, ops);
	WriteOut* w = new WriteOut();
	iopipe[pipeId] = new Pipe(PIPE_SIZE);
	ops[pipeId] = w;
	w->Run(*iopipe[pipe], output, *sch);
}

void ProjectNode::execute(Pipe** iopipe, RelationalOp** ops) {
	//cout<<"\nexecuting project node";
	child -> execute(iopipe, ops);
	Project* proj = new Project();
	iopipe[pipeId] = new Pipe(PIPE_SIZE);
	ops[pipeId] = proj;
	proj->Run(*iopipe[pipe], *iopipe[pipeId], finalAtts, inAtts, outAtts);
}
