#include <boost/config.hpp>
#include <boost/version.hpp>
#if BOOST_VERSION >= 105600 && (! defined(BOOST_GCC) || BOOST_GCC >= 40500)

#include <CGAL/IO/WKT.h>
#include <CGAL/Simple_cartesian.h>
#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Arr_linear_traits_2.h>
#include <CGAL/Arr_segment_traits_2.h>
#include <CGAL/Arr_polyline_traits_2.h>
#include <CGAL/Arrangement_2.h>
#include <CGAL/Arr_extended_dcel.h>
#include <CGAL/Arr_overlay_2.h>
#include <CGAL/Arr_default_overlay_traits.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <string>

#include "dcels.h"
#include "arr_print.h"

// Adding a timer function...
#include <cstdlib>
#include <sys/timeb.h>
int getMilliCount(){
	timeb tb;
	ftime(&tb);
	int nCount = tb.millitm + (tb.time & 0xfffff) * 1000;
	return nCount;
}

int getMilliSpan(int nTimeStart){
	int nSpan = getMilliCount() - nTimeStart;
	if(nSpan < 0)
		nSpan += 0x100000 * 1000;
	return nSpan;
}

// Defining a functor for creating the face label...
struct Overlay_label{
  string operator() (string a, string b) const {
    return a + "|" + b;
  }
};


int main(int argc, char* argv[]) {
  
  // General definitions...
  typedef CGAL::Exact_predicates_exact_constructions_kernel Kernel;
  typedef CGAL::Arr_segment_traits_2<Kernel>                Segment_traits_2;
  typedef CGAL::Arr_polyline_traits_2<Segment_traits_2>     Traits_2;
  typedef Traits_2::Point_2                                 Point_2;
  typedef Traits_2::Segment_2                               Segment_2;
  typedef Traits_2::Curve_2                                 Polyline_2;

  // DCELs definitions...
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
  // WKT reader definitions...
  typedef CGAL::Polygon_with_holes_2<Kernel>                  Polygon;
  typedef CGAL::Polygon_2<Kernel>::Vertex_iterator            vertexIt;
  typedef Polygon::Hole_iterator                              holeIt;

  {
    if (argc < 3) {
      cerr << "usage: " << argv[0] << " file1 file2" << endl;
      exit(-1);
    }

    //-------------------------------------------------------------------------------------------------
    ifstream isA( argv[1] );
    list<Polygon> polys;
    Traits_2 traits;
    ArrangementA_2 arr1(&traits);
    Traits_2::Construct_curve_2 polyline_construct = traits.construct_curve_2_object();

    // Reading WKT file...
    std::cout << "Reading A polygons..." << std::endl;
    do {
      Polygon p;
      CGAL::read_polygon_WKT(isA, p);
      if(!p.outer_boundary().is_empty())
	polys.push_back(p);
    } while(isA.good() && !isA.eof());

    // Inserting as polyline...
    std::cout << "Inserting polygons..." << std::endl;
    int start = getMilliCount();
    int edges = 0;
    for(Polygon p : polys){
      std::vector<Point_2> points;
      for (vertexIt vi = p.outer_boundary().begin(); vi != p.outer_boundary().end(); ++vi){
	points.push_back(*vi);
      }
      points.push_back(points[0]);
      Polyline_2 lines = polyline_construct(points.begin(), points.end());
      insert(arr1, lines);
      //std::cout << "Polygon added. " << points.size() << " segments." << std::endl;
      edges += points.size();
    }

    // Assigning face IDs...
    ArrangementA_2::Face_iterator ait;
    int n = 0;
    for (ait = arr1.faces_begin(); ait != arr1.faces_end(); ++ait){
      string id = "A" + boost::lexical_cast<string>(n);;
      ait->set_data(id);
    
      n++;
    }
    cout << "Done with arr1." << endl;
    int milliSecondsElapsed = getMilliSpan(start);
    std::cout << "Time for A: " << milliSecondsElapsed / 1000.0 << " s." << std::endl;
    //------------------------------------------------------------------------------------------------

    ifstream isB( argv[2] );
    ArrangementB_2 arr2(&traits);
    polys.clear();

    // Reading WKT file...
    std::cout << "Reading B polygons..." << std::endl;
    do {
      Polygon p;
      CGAL::read_polygon_WKT(isB, p);
      if(!p.outer_boundary().is_empty())
	polys.push_back(p);
    } while(isB.good() && !isB.eof());

    // Inserting as polyline...
    std::cout << "Inserting polygons..." << std::endl;
    start = getMilliCount();
    edges = 0;
    for(Polygon p : polys){
      std::vector<Point_2> points;
      for (vertexIt vi = p.outer_boundary().begin(); vi != p.outer_boundary().end(); ++vi){
	points.push_back(*vi);
      }
      points.push_back(points[0]);
      Polyline_2 lines = polyline_construct(points.begin(), points.end());
      insert(arr2, lines);
      //std::cout << "Polygon added. " << points.size() << " segments." << std::endl;
      edges += points.size();
    }

    // Assigning face IDs...
    ArrangementB_2::Face_iterator bit;
    int m = 0;
    for (bit = arr2.faces_begin(); bit != arr2.faces_end(); ++bit){
      string id = "B" + boost::lexical_cast<string>(m);;
      bit->set_data(id);
      m++;
    }
    cout << "Done with arr2." << endl;
    milliSecondsElapsed = getMilliSpan(start);
    std::cout << "Time for B: " << milliSecondsElapsed / 1000.0 << " s." << std::endl;
    //------------------------------------------------------------------------------------------------

    // Compute the overlay of the two arrangements.
    std::cout << "Computing overlay..." << std::endl;
    start = getMilliCount();
    ArrangementRes_2 overlay_arr;
    Overlay_traits   overlay_traits;

    overlay (arr1, arr2, overlay_arr, overlay_traits);

    milliSecondsElapsed = getMilliSpan(start);
    std::cout << "Time for overlay: " << milliSecondsElapsed / 1000.0 << " s." << std::endl;
    
    // Go over the faces of the overlaid arrangement and their labels.
    ArrangementRes_2::Face_iterator  res_fit;

    ofstream wkt;
    wkt.open ("faces.wkt");
    cout << "Saving faces to faces.wkt..." << endl;
    for (res_fit = overlay_arr.faces_begin(); res_fit != overlay_arr.faces_end(); ++res_fit){
      if(!res_fit->is_unbounded()){
	string w = get_face_wkt<ArrangementRes_2> (res_fit);
	//cout << w;
	wkt << w;
      }
    }
    wkt.close();
    cout << "Done!!!" << endl;
  }
  
  return 0;
}
#else
int main()
{
  return 0;
}
#endif
