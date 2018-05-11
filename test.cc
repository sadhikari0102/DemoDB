
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlan.h"
#include "DBEngine.h"

using namespace std;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern struct AttrList *attrs;
extern struct NameList *sort;
extern char *table, *file, *oldtable, *output;

char* catalog_path;
char* dbfile_dir;
char* tpch_dir;
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);

int main () {
	const char *settings = "test.cat";
	FILE *fp = fopen(settings, "r");
	if (fp) {
		char *mem = (char *) malloc (80 * 3);
		catalog_path = &mem[0];
		dbfile_dir = &mem[80];
		tpch_dir = &mem[160];
		char line[80];
		fgets (line, 80, fp);
		sscanf (line, "%s\n", catalog_path);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", dbfile_dir);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", tpch_dir);
		fclose (fp);
		if (! (catalog_path && dbfile_dir && tpch_dir)) {
			cerr << " Test settings file 'test.cat' not in correct format.\n";
			free (mem);
			exit (1);
		}
	}
	//cout<<"\n"<<catalog_path<<"\n"<<dbfile_dir<<"\n"<<tpch_dir;

	char* filename = "Statistics.txt";
	Statistics s;
	QueryPlan plan(&s);
	DBEngine engine;
	//cout<<"\n>";
	/*
	char* cnf = "SELECT SUM (ps.ps_supplycost), s.s_suppkey "
			"FROM part AS p, supplier AS s, partsupp AS ps "
			"WHERE (p.p_partkey = ps.ps_partkey) AND"
			"(s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500.0) "
			"GROUP BY s.s_suppkey"; */

	/*
	char* cnf = "SELECT SUM(l.l_discount) "
			"FROM customer AS c, orders AS o, lineitem AS l "
			"WHERE (c.c_custkey = o.o_custkey) AND "
			"(o.o_orderkey = l.l_orderkey) AND "
			"(c.c_name = 'Customer#000070919') AND "
			"(l.l_quantity > 30.0) AND (l.l_discount < 0.03)";*/


	/*char* cnf = "SELECT l.l_orderkey "
			"FROM lineitem AS l "
			"WHERE (l.l_quantity > 30.0)";*/

/*
	char* cnf = "SELECT SUM (l.l_extendedprice * l.l_discount) "
			"FROM lineitem AS l "
			"WHERE (l.l_discount<0.07) AND (l.l_quantity < 24)";*/

	/*char* cnf = "SELECT l.l_discount "
			"FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r "
			"WHERE (l.l_orderkey = o.o_orderkey) AND "
			"(o.o_custkey = c.c_custkey) AND "
			"(c.c_nationkey = n.n_nationkey) AND "
			"(n.n_regionkey = r.r_regionkey) AND "
			"(r.r_regionkey = 1) AND (o.orderkey < 10000)";

	cout<<"\n\n***QUERY***\n\n";
	cout<<cnf;
	yy_scan_string(cnf);*/

	while(true) {

		cout<<"\nEnter Query >";
		if(yyparse()!=0) {
			cout<<"\nError while parsing CNF.\n";
			exit(0);
		}
		s.Read(filename);

		if(table) {
			if(engine.Create())
				cout<<"\nTable "<<table<<" created!";
			else
				cout<<"\nTable "<<table<<" already exist! TRY AGAIN.";
		}
		else if(oldtable && file) {
			if(engine.Insert())
				cout<<"\nInserted into "<<oldtable;
			else
				cout<<"\nInsert in "<<oldtable<<" failed! TRY AGAIN.";
		}
		else if(oldtable) {
			if(engine.Drop())
				cout<<"\nTable "<<oldtable<<" dropped!";
			else
				cout<<"\nError in dropping table "<<oldtable<<"! Check if table already exists and TRY AGAIN.";
		}
		else if(finalFunction || attsToSelect) {
			plan.PlanQuery();
			plan.PrintPlan(cout);
			plan.ExecuteQuery();
		}
		else if(output)
			plan.AssignOut(output);

		attrs = NULL;
		finalFunction = NULL;
		tables = NULL;
		boolean = NULL;
		groupingAtts = NULL;
		attsToSelect = NULL;
		table = NULL;
		oldtable = NULL;
		file = NULL;
		output = NULL;
		distinctAtts=0;
		distinctFunc = 0;
		//s.Print();
	}

}
