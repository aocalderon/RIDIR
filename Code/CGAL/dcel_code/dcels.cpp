// Based on examples/Arrangement_on_surface_2/overlay_unbounded.cpp
// A face overlay of two arrangements with unbounded faces.

#include <boost/config.hpp>
#include <boost/version.hpp>
#if BOOST_VERSION >= 105600 && (! defined(BOOST_GCC) || BOOST_GCC >= 40500)
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/lexical_cast.hpp>

#include <CGAL/Simple_cartesian.h>
#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Arr_linear_traits_2.h>
#include <CGAL/Arrangement_2.h>
#include <CGAL/Arr_extended_dcel.h>
#include <CGAL/Arr_overlay_2.h>
#include <CGAL/Arr_default_overlay_traits.h>
#include <CGAL/IO/WKT.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <string>

#include "dcels.h"
#include "arr_print.h"

using namespace std;
namespace bg = boost::geometry;

typedef bg::model::d2::point_xy<double> point_type;

vector<vector<string>> read_tsv(string fname) {
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

// Define a functor for creating a label from a character and an integer.
struct Overlay_label{
  string operator() (string a, string b) const {
    return a + "|" + b;
  }
};

typedef CGAL::Exact_predicates_exact_constructions_kernel   Kernel;
//typedef CGAL::Simple_cartesian<CGAL::Gmpq>                  Kernel;
typedef CGAL::Arr_linear_traits_2<Kernel>                   Traits_2;
typedef Traits_2::Point_2                                   Point_2;
typedef Traits_2::Segment_2                                 Segment_2;
typedef Traits_2::Ray_2                                     Ray_2;
typedef Traits_2::Line_2                                    Line_2;
typedef Traits_2::X_monotone_curve_2                        X_monotone_curves_2;
typedef CGAL::Arr_face_extended_dcel<Traits_2, string>      DcelA;
typedef CGAL::Arrangement_2<Traits_2, DcelA>                ArrangementA_2;
typedef CGAL::Arr_face_extended_dcel<Traits_2, string>      DcelB;
typedef CGAL::Arrangement_2<Traits_2, DcelB>                ArrangementB_2;
typedef CGAL::Arr_face_extended_dcel<Traits_2, string>      DcelRes;
typedef CGAL::Arrangement_2<Traits_2, DcelRes>              ArrangementRes_2;
typedef CGAL::Arr_face_overlay_traits<ArrangementA_2,
                                      ArrangementB_2,
                                      ArrangementRes_2,
                                      Overlay_label>        Overlay_traits;

int main (int argc, char* argv[]){
  if (argc < 3) {
    cerr << "usage: " << argv[0] << " file1 file2" << endl;
    exit(-1);
  }
  
  string filename1 = argv[1];
  vector<vector<string>> data1 = read_tsv(filename1);
  ArrangementA_2 arr1;
  vector<array<double, 2>> points;

  for (int i; i < data1.size(); ++i) {
    bg::model::polygon<point_type> poly;

    
    bg::read_wkt(data1[i][0], poly);
    for(auto it = boost::begin(bg::exterior_ring(poly)); it != boost::end(bg::exterior_ring(poly)); ++it){
      double x = bg::get<0>(*it);
      double y = bg::get<1>(*it);
      array<double, 2> point = {x, y};
      points.push_back(point);
    }
    cout << "Points read " << data1[i][0]  << endl;
    for (int i = 0; i < points.size() - 1; ++i) {
      array<double, 2> p1 = points[i];
      array<double, 2> p2 = points[i + 1];
    
      Segment_2 s (Point_2(p1.at(0), p1.at(1)), Point_2(p2.at(0), p2.at(1)));
      cout << s << endl;
      insert (arr1, s);
    }
    cout << "Segments added" << endl;
    points.clear();
  }

  ArrangementA_2::Face_iterator ait;
  int n = 0;
  for (ait = arr1.faces_begin(); ait != arr1.faces_end(); ++ait){
    string id = "A" + boost::lexical_cast<string>(n);;
    ait->set_data(id);
    
    n++;
  }
  cout << "Done with arr1." << endl;
  //-----------------------------------------------------------------------------------------------------------
  
  string filename2 = argv[2];
  vector<vector<string>> data2 = read_tsv(filename2);
  ArrangementB_2 arr2;
  points.clear();

  for (int i; i < data2.size(); ++i) {
    bg::model::polygon<point_type> poly;
    bg::read_wkt(data2[i][0], poly);
    for(auto it = boost::begin(bg::exterior_ring(poly)); it != boost::end(bg::exterior_ring(poly)); ++it)
      {
        double x = bg::get<0>(*it);
	double y = bg::get<1>(*it);
	array<double, 2> point = {x, y};
	points.push_back(point);
      }    
    cout << "Points read " << data2[i][0]  << endl;
    for (int i = 0; i < points.size() - 1; ++i) {
      array<double, 2> p1 = points[i];
      array<double, 2> p2 = points[i + 1];
    
      Segment_2 s (Point_2(p1.at(0), p1.at(1)), Point_2(p2.at(0), p2.at(1)));
      cout << s << endl;
      insert (arr2, s);
    }
    cout << "Segments added" << endl;
    points.clear();
  }

  ArrangementB_2::Face_iterator bit;
  int m = 0;
  for (bit = arr2.faces_begin(); bit != arr2.faces_end(); ++bit){
    string id = "B" + boost::lexical_cast<string>(m);;
    bit->set_data(id);
    m++;
  }
  cout << "Done with arr2." << endl;
  //-----------------------------------------------------------------------------------------------------------

  // Compute the overlay of the two arrangements.
  ArrangementRes_2 overlay_arr;
  Overlay_traits   overlay_traits;

  overlay (arr1, arr2, overlay_arr, overlay_traits);

  // Go over the faces of the overlaid arrangement and their labels.
  ArrangementRes_2::Face_iterator  res_fit;

  ofstream wkt;
  wkt.open ("faces.wkt");
  cout << "The overlay faces are: " << endl;
  for (res_fit = overlay_arr.faces_begin(); res_fit != overlay_arr.faces_end(); ++res_fit){
    if(!res_fit->is_unbounded()){
      string w = get_face_wkt<ArrangementRes_2> (res_fit);
      cout << w;
      wkt << w;
    }
  }
  wkt.close();

  return 0;
}
#else
int main()
{
  return 0;
}
#endif
