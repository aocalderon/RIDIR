#include <boost/config.hpp>
#include <boost/version.hpp>
#if BOOST_VERSION >= 105600 && (! defined(BOOST_GCC) || BOOST_GCC >= 40500)

#include <CGAL/IO/WKT.h>
#include <CGAL/Simple_cartesian.h>
#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Arr_linear_traits_2.h>
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

using namespace std;

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
  typedef CGAL::Exact_predicates_exact_constructions_kernel   Kernel;
  //typedef CGAL::Simple_cartesian<CGAL::Gmpq>                  Kernel;
  typedef CGAL::Arr_linear_traits_2<Kernel>                   Traits_2;
  typedef Traits_2::Point_2                                   Point_2;
  typedef Traits_2::Segment_2                                 Segment_2;

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

    cout << "Starting timer..." << endl;
    int start = getMilliCount();

    //-----------------------------------------------------------------------------------------------------------
    ifstream isA( argv[1] );
    ArrangementA_2 arr1;
    list<Polygon> polys;

    // Reading WKT file...
    do {
      Polygon p;
      CGAL::read_polygon_WKT(isA, p);
      if(!p.outer_boundary().is_empty())
	polys.push_back(p);
    } while(isA.good() && !isA.eof());
    // Extracting segments...
    for(Polygon p : polys){
      vector<Point_2> points;
      for (vertexIt vi = p.outer_boundary().begin(); vi != p.outer_boundary().end(); ++vi){
	points.push_back(*vi);
      }
      points.push_back(points[0]);
      // Outer segments...
      int n=0;
      //cout << "Outer: " << endl;
      for(int i = 0; i < points.size() - 1; ++i){
	//cout << "Segment " << n++ <<  " (" << points[i] << ", " << points[i + 1] << ")" << endl;
	Point_2 p1 = points[i];
	Point_2 p2 = points[i + 1];
	Segment_2 s (p1, p2);
	//cout << s << endl;
	insert (arr1, s);
      }
      cout << "Polygon added. " << points.size() << " segments." << endl;
      // Inner segments...
      if(p.has_holes()){
	//cout << "Hole(s): " << endl;
	for (holeIt hit = p.holes_begin(); hit != p.holes_end(); ++hit){
	  points.clear();
	  for (vertexIt vi = (*hit).begin(); vi != (*hit).end(); ++vi){
	    points.push_back(*vi);
	  }
	  points.push_back(points[0]);
	  
	  for(int i = 0; i < points.size() - 1; ++i){
	    //cout << "Segment " << n++ <<  " (" << points[i] << ", " << points[i + 1] << ")" << endl;
	    Point_2 p1 = points[i];
	    Point_2 p2 = points[i + 1];
	    Segment_2 s (p1, p2);
	    //cout << s << endl;
	    insert (arr1, s);
	  }
	  cout << "Hole added. " << points.size() << " segments." << endl;
	}
      }
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
    //-----------------------------------------------------------------------------------------------------------

    ifstream isB( argv[2] );
    ArrangementA_2 arr2;
    polys.clear();

    // Reading WKT file...
    do {
      Polygon p;
      CGAL::read_polygon_WKT(isB, p);
      if(!p.outer_boundary().is_empty())
	polys.push_back(p);
    } while(isB.good() && !isB.eof());
    // Extracting segments...
    for(Polygon p : polys){
      vector<Point_2> points;
      for (vertexIt vi = p.outer_boundary().begin(); vi != p.outer_boundary().end(); ++vi){
	points.push_back(*vi);
      }
      points.push_back(points[0]);
      // Outer segments...
      int n = 0;
      //cout << "Outer: " << endl;
      for(int i = 0; i < points.size() - 1; ++i){
	//cout << "Segment " << n++ <<  " (" << points[i] << ", " << points[i + 1] << ")" << endl;
	Point_2 p1 = points[i];
	Point_2 p2 = points[i + 1];
	Segment_2 s (p1, p2);
	//cout << s << endl;
	insert (arr2, s);
      }
      cout << "Polygon added. " << points.size() << " segments." << endl;
      // Inner segments...
      if(p.has_holes()){
	//cout << "Hole(s): " << endl;
	for (holeIt hit = p.holes_begin(); hit != p.holes_end(); ++hit){
	  points.clear();
	  for (vertexIt vi = (*hit).begin(); vi != (*hit).end(); ++vi){
	    points.push_back(*vi);
	  }
	  points.push_back(points[0]);
	  
	  for(int i = 0; i < points.size() - 1; ++i){
	    //cout << "Segment " << n++ <<  " (" << points[i] << ", " << points[i + 1] << ")" << endl;
	    Point_2 p1 = points[i];
	    Point_2 p2 = points[i + 1];
	    Segment_2 s (p1, p2);
	    //cout << s << endl;
	    insert (arr2, s);
	  }
	  cout << "Hole added: " << points.size() << " segments." << endl;
	}
      }
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
    //-----------------------------------------------------------------------------------------------------------

    // Compute the overlay of the two arrangements.
    ArrangementRes_2 overlay_arr;
    Overlay_traits   overlay_traits;

    overlay (arr1, arr2, overlay_arr, overlay_traits);

    int milliSecondsElapsed = getMilliSpan(start);
    cout << "Total time: " << milliSecondsElapsed << endl;
    
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
