//! \file examples/Arrangement_on_surface_2/overlay_unbounded.cpp
// A face overlay of two arrangements with unbounded faces.

#include <string>
#include <boost/lexical_cast.hpp>

#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Arr_linear_traits_2.h>
#include <CGAL/Arrangement_2.h>
#include <CGAL/Arr_extended_dcel.h>
#include <CGAL/Arr_overlay_2.h>
#include <CGAL/Arr_default_overlay_traits.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <iostream>

#include "dcels.h"

using namespace std;
namespace bg = boost::geometry;

typedef bg::model::d2::point_xy<double> point_type;
bg::model::polygon<point_type> poly;

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

int main ()
{
  string filename1 = "arr1.wkt";
  vector<vector<string>> data1 = read_tsv(filename1);
  vector<array<double, 2>> points;
  for (int i; i < data1.size(); ++i) {
    bg::read_wkt(data1[i][0], poly);

    //getting the vertices back
    for(auto it = boost::begin(bg::exterior_ring(poly)); it != boost::end(bg::exterior_ring(poly)); ++it)
      {
        double x = bg::get<0>(*it);
	double y = bg::get<1>(*it);
	array<double, 2> point = {x, y};
	points.push_back(point);
	//cout << "(" << point.at(0) << ", " << point.at(1) << ")"<< endl;
      }    

    //cout << bg::num_points(poly) << endl;
  }
  
  ArrangementA_2 arr1;
  
  for (int i = 0; i < points.size() - 1; ++i) {
    array<double, 2> p1 = points[i];
    array<double, 2> p2 = points[i + 1];
    
    Segment_2 s (Point_2(p1.at(0), p1.at(1)), Point_2(p2.at(0), p2.at(1)));
    cout << s << endl;
    insert_non_intersecting_curve (arr1, s);
  }

  CGAL_assertion (arr1.number_of_faces() == 2);

  ArrangementA_2::Face_iterator ait;
  int n = 0;
  for (ait = arr1.faces_begin(); ait != arr1.faces_end(); ++ait){
    string id = "A" + boost::lexical_cast<string>(n);;
    ait->set_data(id);
    n++;
  }
  cout << "Done with arr1." << endl;

  // Construct the second arrangement, containing a single square-shaped face.
  ArrangementB_2 arr2;

  Segment_2 t1 (Point_2(4, 1), Point_2(7, 4));
  Segment_2 t2 (Point_2(7, 4), Point_2(4, 7));
  Segment_2 t3 (Point_2(4, 7), Point_2(1, 4));
  Segment_2 t4 (Point_2(1, 4), Point_2(4, 1));

  insert_non_intersecting_curve (arr2, t1);
  insert_non_intersecting_curve (arr2, t2);
  insert_non_intersecting_curve (arr2, t3);
  insert_non_intersecting_curve (arr2, t4);
  
  CGAL_assertion (arr2.number_of_faces() == 2);

  ArrangementB_2::Face_iterator bit;
  int m = 0;
  for (bit = arr2.faces_begin(); bit != arr2.faces_end(); ++bit){
    string id = "B" + boost::lexical_cast<string>(m);;
    bit->set_data(id);
    m++;
  }
  cout << "Done with arr2." << endl;

  // Compute the overlay of the two arrangements.
  ArrangementRes_2       overlay_arr;
  Overlay_traits         overlay_traits;

  overlay (arr1, arr2, overlay_arr, overlay_traits);

  // Go over the faces of the overlaid arrangement and their labels.
  ArrangementRes_2::Face_iterator  res_fit;

  std::cout << "The overlay faces are: ";
  for (res_fit = overlay_arr.faces_begin();
       res_fit != overlay_arr.faces_end(); ++res_fit)
  {
    std::cout << res_fit->data() << " ("
              << (res_fit->is_unbounded() ? "unbounded" : "bounded")
              << ")." << std::endl;
  }

  return 0;
}
