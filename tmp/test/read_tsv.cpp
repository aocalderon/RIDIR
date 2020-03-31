#include <iostream>
#include <iomanip>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <cmath>
#include <cstring>
#include <string>
#include <ctime>
#include <vector>
#include <list>
#include <map>
#include <set>
#include <deque>
#include <stack>
#include <queue>
#include <bitset>
#include <algorithm>
#include <numeric>
#include <utility>
#include <sstream>
#include <boost/algorithm/string/trim.hpp>

#define ABS(a) ((a) < 0 ? - (a) : (a))
#define FOR(i,a,b) for(int i=(a);i<(b);++i)
#define REP(i,n)  FOR(i,0,n)

using namespace std;

vector<vector<string>> items;

vector<vector<string>> read_tsv(char* fname) {
	ifstream ifs(fname);
	if (ifs.fail()) {
		cerr << "error" << endl;
		exit(-1);
	}
	std::string line;
	while (getline(ifs, line)) {
		stringstream ss(line);
		vector<string> item;
		string tmp;
		while(getline(ss, tmp, '\t')) {
			item.push_back(tmp);
		}
		items.push_back(item);
	}
	return items;
}

int main(int argc, char* argv[]) {
	if (argc < 2) {
		cerr << "usage: " << argv[0] << " file" << endl;
		exit(-1);
	}
	
	vector<vector<string>> items = read_tsv(argv[1]);
	
	REP(i, items.size()){
	  string wkt  = items[i][0];
	  string id = items[i][1];
	  stringstream wkt_stream(wkt);
	  vector<string> points;
	  string tmp_point;
	  
	  while(getline(wkt_stream, tmp_point, ',')){
	    points.push_back(tmp_point);
	  }
	  
	  REP(j, points.size()){
	    boost::algorithm::trim(points[j]);
	    if(j == 0) points[j].erase(0, 9);
	    if(j == points.size() - 1) points[j].erase(points[j].size() - 2, points[j].size());
	    cout << id << "\t" << points[j] << endl;
	  }
	}
	
	return 0;
}
