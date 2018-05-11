CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif


a4-2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapDBFile.o SortedDBFile.o RecordBuffer.o Statistics.o QueryPlan.o Node.o DBEngine.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o lex.yy.o test.o
	$(CC) -o a4-2.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapDBFile.o SortedDBFile.o RecordBuffer.o Statistics.o QueryPlan.o Node.o DBEngine.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o lex.yy.o test.o -lfl -lpthread

#a4-2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapDBFile.o SortedDBFile.o RecordBuffer.o Statistics.o QueryPlan.o Node.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o lex.yy.o test1.o
#	$(CC) -o a4-2.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapDBFile.o SortedDBFile.o RecordBuffer.o Statistics.o QueryPlan.o Node.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o lex.yy.o test1.o -lfl -lpthread

#a4-2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapDBFile.o SortedDBFile.o RecordBuffer.o Statistics.o QueryPlan.o Node.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o lex.yy.o testCreate.o
#	$(CC) -o a4-2.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapDBFile.o SortedDBFile.o RecordBuffer.o Statistics.o QueryPlan.o Node.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o lex.yy.o testCreate.o -lfl -lpthread

#a4-2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapDBFile.o SortedDBFile.o RecordBuffer.o Statistics.o QueryPlan.o Node.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o lex.yy.o testQuery.o
#	$(CC) -o a4-2.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o HeapDBFile.o SortedDBFile.o RecordBuffer.o Statistics.o QueryPlan.o Node.o Pipe.o BigQ.o RelOp.o Function.o y.tab.o lex.yy.o testQuery.o -lfl -lpthread
	
test.o: test.cc
	$(CC) -g -c test.cc

test1.o: test1.cc
	$(CC) -g -c test1.cc

testCreate.o: testCreate.cc
	$(CC) -g -c testCreate.cc

testQuery.o: testQuery.cc
	$(CC) -g -c testQuery.cc

main:   y.tab.o lex.yy.o main.o
	$(CC) -o main y.tab.o lex.yy.o main.o -lfl
	
main.o : main.cc
	$(CC) -g -c main.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc
	
QueryPlan.o: QueryPlan.cc
	$(CC) -g -c QueryPlan.cc
	
Node.o: Node.cc
	$(CC) -g -c Node.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc
	
SortedDBFile.o: SortedDBFile.cc SortedDBFile.h
	$(CC)  -c SortedDBFile.cc

HeapDBFile.o: HeapDBFile.cc HeapDBFile.h
	$(CC)  -c HeapDBFile.cc
	
RecordBuffer.o: RecordBuffer.cc RecordBuffer.h
	$(CC)  -c RecordBuffer.cc 

DBEngine.o: DBEngine.cc DBEngine.h
	$(CC)  -c DBEngine.cc 

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
	
lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c
	
clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.*
	rm -f y.tab.h
