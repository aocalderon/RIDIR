#include "dcels.h"

vector<vector<string>> read_tsv(char* fname) {
  vector<vector<string>> items;
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

  const string s = "(2 2, 6 2, 6 6, 6 2, 2 2)";
  regex rgx("\\(\\w+\\)");
  smatch match;

  if (std::regex_search(s.begin(), s.end(), match, rgx))
    std::cout << "match: " << match[1] << '\n';
	
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
      if(j == 0) points[j].erase(0, 9);
      if(j == points.size() - 1) points[j].erase(points[j].size() - 2, points[j].size());
      cout << id << "\t" << points[j] << endl;
    }
  }
	
  return 0;
}
