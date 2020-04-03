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

