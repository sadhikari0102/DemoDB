/*
 * RecordBuffer.cc
 *
 *  Created on: Mar 26, 2018
 *      Author: adhikari
 */
#include "RecordBuffer.h"

RecordBuffer :: RecordBuffer(size_t pages) {
	size = 0;
	capacity = pages*PAGE_SIZE;
	count=0;
	data = new Record[capacity/sizeof(Record*)];
}

RecordBuffer :: ~RecordBuffer() {
	delete[] data;
}

int RecordBuffer :: Insert(Record &addme) {
	size+=addme.GetSize();
	if(size>capacity)
		return 0;
	else {
		data[count].Consume(&addme);
		count++;
		return 1;
	}
}

void RecordBuffer :: Refresh() {
	count=0;
	size=0;
	data = new Record[capacity/sizeof(Record*)];
}


