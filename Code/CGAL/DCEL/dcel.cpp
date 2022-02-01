#include <boost/config.hpp>
#include <boost/version.hpp>
#include <boost/tokenizer.hpp>
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

#include <ctype.h>
#include <unistd.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <ctime>
#include <chrono>
#include <iomanip>

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

std::string getTime(){
  using namespace std::chrono;

  auto now = system_clock::now();
  // get number of milliseconds for the current second (remainder after division into seconds).
  auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
  // convert to std::time_t in order to convert to std::tm (broken time)
  auto timer = system_clock::to_time_t(now); // convert to broken time
  std::tm bt = *std::localtime(&timer);
  std::ostringstream oss;
  oss << std::put_time(&bt, "%Y-%m-%d %H:%M:%S"); 	
  oss << '.' << std::setfill('0') << std::setw(3) << ms.count() << "|";

  return oss.str();
}

// Defining a functor for creating the face label...
struct Overlay_label{
  string operator() (string a, string b) const {
    return a + " " + b;
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
    int debug = 0;
    char* filename1;
    char* filename2;
    char operation = 0;
    int precision = 5;
    
    int c;
    while ((c = getopt (argc, argv, "a:b:o:d")) != -1){
      switch (c){
      case 'd':
	debug = 1;
        break;
      case 'a':
	if(optarg) filename1 = optarg;
        break;
      case 'b':
	if(optarg) filename2 = optarg; 
        break;
      case 'o':
	if(optarg) operation = optarg[0]; 
        break;
      case '?':
	std::cout << "./dcel -a <filename1> -b <filename2> [-o <operator>] [-d]" << std::endl;
        return 1;
      default:
	abort ();
      }
    }

    //

    //-------------------------------------------------------------------------------------------------
    ifstream isA( filename1 );
    list<Polygon> polys;
    Traits_2 traits;
    ArrangementA_2 arr1(&traits);
    Traits_2::Construct_curve_2 polyline_construct = traits.construct_curve_2_object();

    // Reading WKT file...
    std::cout << getTime() << "Reading A polygons..." << std::endl;
    do {
      Polygon p;
      CGAL::read_polygon_WKT(isA, p);
      if(!p.outer_boundary().is_empty())
	polys.push_back(p);
    } while(isA.good() && !isA.eof());

    // Inserting as polyline...
    std::cout << getTime() << "Inserting polygons..." << std::endl;
    int start = getMilliCount();
    int edges = 0;
    int npolys = polys.size();
    int ipolys = 1;
    for(Polygon p : polys){
      std::vector<Point_2> points;
      for (vertexIt vi = p.outer_boundary().begin(); vi != p.outer_boundary().end(); ++vi){
	points.push_back(*vi);
      }
      points.push_back(points[0]);
      Polyline_2 lines = polyline_construct(points.begin(), points.end());
      insert(arr1, lines);
      edges += points.size();
      if(ipolys % 1000 == 0){
	std::cout << getTime() << "Polygon added (" << ipolys << "/" << npolys << ")." << std::endl;
      }
      ipolys++;
    }
    std::cout << getTime() << "Insertion done! [" << npolys << " polygons|" << edges << " segments]." << std::endl;      
    
    // Assigning face IDs...
    ArrangementA_2::Face_iterator ait;
    int n = 0;
    for (ait = arr1.faces_begin(); ait != arr1.faces_end(); ++ait){
      if(!ait->is_unbounded()){
	string id = "A" + boost::lexical_cast<string>(n);
	ait->set_data(id);
    
	n++;
      }
    }
    std::cout << getTime() << "Done with A [" << edges << " edges]." << endl;
    int milliSecondsElapsed = getMilliSpan(start);
    double timeA = milliSecondsElapsed / 1000.0;
    std::cout << getTime() << "Time for A: " << timeA << " s." << std::endl;

    if(debug){
      ArrangementA_2::Face_iterator  a_fit;
      ofstream wkt;
      wkt.open ("A.wkt");
      std::cout << getTime() << "Saving faces to A.wkt..." << endl;
      int nfaces = 0;
      for (a_fit = arr1.faces_begin(); a_fit != arr1.faces_end(); ++a_fit){
	if(!a_fit->is_unbounded()){
	  string w = get_face_wkt2<ArrangementRes_2> (a_fit, precision);
	  wkt << w;
	  nfaces++;
	}
      }
      wkt.close();
      std::cout << getTime() << "Done! " << nfaces << " faces saved." << std::endl;
    }
    
    //------------------------------------------------------------------------------------------------

    ifstream isB( filename2 );
    ArrangementB_2 arr2(&traits);
    polys.clear();

    // Reading WKT file...
    std::cout << getTime() << "Reading B polygons..." << std::endl;
    do {
      Polygon p;
      CGAL::read_polygon_WKT(isB, p);
      if(!p.outer_boundary().is_empty())
	polys.push_back(p);
    } while(isB.good() && !isB.eof());

    // Inserting as polyline...
    std::cout << getTime() << "Inserting polygons..." << std::endl;
    start = getMilliCount();
    edges = 0;
    npolys = polys.size();
    ipolys = 1;
    for(Polygon p : polys){
      std::vector<Point_2> points;
      for (vertexIt vi = p.outer_boundary().begin(); vi != p.outer_boundary().end(); ++vi){
	points.push_back(*vi);
      }
      points.push_back(points[0]);
      Polyline_2 lines = polyline_construct(points.begin(), points.end());
      insert(arr2, lines);
      edges += points.size();
      if(ipolys % 1000 == 0){
	std::cout << getTime() << "Polygon added (" << ipolys << "/" << npolys << ")." << std::endl;
      }
      ipolys++;
    }
    std::cout << getTime() << "Insertion done! [" << npolys << " polygons|" << edges << " segments]." << std::endl;      

    // Assigning face IDs...
    ArrangementB_2::Face_iterator bit;
    int m = 0;
    for (bit = arr2.faces_begin(); bit != arr2.faces_end(); ++bit){
      if(!bit->is_unbounded()){
	string id = "B" + boost::lexical_cast<string>(m);;
	bit->set_data(id);
	m++;
      }
    }
    std::cout << getTime() << "Done with B [" << edges << " edges]." << endl;
    milliSecondsElapsed = getMilliSpan(start);
    double timeB = milliSecondsElapsed / 1000.0;
    std::cout << getTime() << "Time for B: " << timeB << " s." << std::endl;
    if(debug){
      ArrangementB_2::Face_iterator  b_fit;
      ofstream wkt;
      wkt.open ("B.wkt");
      std::cout << getTime() << "Saving faces to B.wkt..." << endl;
      int nfaces = 0;
      for (b_fit = arr2.faces_begin(); b_fit != arr2.faces_end(); ++b_fit){
	if(!b_fit->is_unbounded()){
	  string w = get_face_wkt2<ArrangementRes_2> (b_fit, precision);
	  wkt << w;
	  nfaces++;
	}
      }
      wkt.close();
      std::cout << getTime() << "Done! " << nfaces << " faces saved." << std::endl;
    }
    
    //------------------------------------------------------------------------------------------------

    // Compute the overlay of the two arrangements.
    std::cout << getTime() << "Computing overlay..." << std::endl;
    start = getMilliCount();
    ArrangementRes_2 overlay_arr;
    Overlay_traits   overlay_traits;

    overlay (arr1, arr2, overlay_arr, overlay_traits);

    milliSecondsElapsed = getMilliSpan(start);
    double timeO = milliSecondsElapsed / 1000.0;
    std::cout << getTime() << "Time for overlay: " << timeO << " s." << std::endl;
    
    if(debug){
      // Go over the faces of the overlaid arrangement and their labels.
      ArrangementRes_2::Face_iterator  res_fit;
      ofstream wkt;
      wkt.open ("faces.wkt");
      std::cout << getTime() << "Saving faces to faces.wkt..." << endl;
      int nfaces = 0;
      for (res_fit = overlay_arr.faces_begin(); res_fit != overlay_arr.faces_end(); ++res_fit){
	if(!res_fit->is_unbounded()){
	  string w = get_face_wkt2<ArrangementRes_2> (res_fit, precision);
	  wkt << w;
	  nfaces++;
	}
      }
      wkt.close();
      std::cout << getTime() << "Done! " << nfaces << " faces saved." << std::endl;
    }

    // Compute an overlay opeartion on the merged arrangement.
    typedef boost::tokenizer<boost::char_separator<char> > tokenizer;

    //
    std::vector<char> opearations {
				   'i',
				   'u',
				   's',
				   'a',
				   'b'};
    //

    for(char operation: opearations){
      std::cout << getTime() << "Computing overlay operation: ";
      start = getMilliCount();

      switch(operation){
      case 'i':
	{
	  string op = "Intersection";
	  std::cout << getTime() << op << std::endl;
	  // Go over the faces of the overlaid arrangement and their labels.
	  ArrangementRes_2::Face_iterator f;
	  ofstream wkt;
	  wkt.open (op + ".wkt");
	  std::cout << getTime() << "Saving faces to " + op + ".wkt..." << endl;
	  int nfaces = 0;
	  for (f = overlay_arr.faces_begin(); f != overlay_arr.faces_end(); ++f){
	    if(!f->is_unbounded()){
	      std::string data = f->data();
	      boost::char_separator<char> sep("|");
	      tokenizer tokens(data, sep);
	      char nlabels = 0;
	      for (tokenizer::iterator it = tokens.begin(); it != tokens.end(); ++it)
		nlabels++;
	      if(nlabels == 2){
		std::string w = get_face_wkt<ArrangementRes_2> (f, precision);
		if(debug)
		  wkt << w;
		nfaces++;
	      }
	    }
	  }
	  wkt.close();
	  std::cout << getTime() << "Done! " << nfaces << " faces saved." << std::endl;
	  break;
	}
      case 'u':
	{
	  string op = "Union";
	  std::cout << getTime() << op << std::endl;
	  // Go over the faces of the overlaid arrangement and their labels.
	  ArrangementRes_2::Face_iterator f;
	  ofstream wkt;
	  wkt.open (op + ".wkt");
	  std::cout << getTime() << "Saving faces to " + op + ".wkt..." << endl;
	  int nfaces = 0;
	  for (f = overlay_arr.faces_begin(); f != overlay_arr.faces_end(); ++f){
	    if(!f->is_unbounded()){
	      std::string data = f->data();
	      boost::char_separator<char> sep("|");
	      tokenizer tokens(data, sep);
	      char nlabels = 0;
	      for (tokenizer::iterator it = tokens.begin(); it != tokens.end(); ++it)
		nlabels++;
	      if(nlabels == 1 || nlabels == 2){
		std::string w = get_face_wkt<ArrangementRes_2> (f, precision);
		if(debug)
		  wkt << w;
		nfaces++;
	      }
	    }
	  }
	  wkt.close();
	  std::cout << getTime() << "Done! " << nfaces << " faces saved." << std::endl;
	  break;
	}
      case 's':
	{
	  string op = "Symmetric";
	  std::cout << getTime() << op << std::endl;
	  // Go over the faces of the overlaid arrangement and their labels.
	  ArrangementRes_2::Face_iterator f;
	  ofstream wkt;
	  wkt.open (op + ".wkt");
	  std::cout << getTime() << "Saving faces to " + op + ".wkt..." << endl;
	  int nfaces = 0;
	  for (f = overlay_arr.faces_begin(); f != overlay_arr.faces_end(); ++f){
	    if(!f->is_unbounded()){
	      std::string data = f->data();
	      boost::char_separator<char> sep("|");
	      tokenizer tokens(data, sep);
	      char nlabels = 0;
	      for (tokenizer::iterator it = tokens.begin(); it != tokens.end(); ++it)
		nlabels++;
	      if(nlabels == 1){
		std::string w = get_face_wkt<ArrangementRes_2> (f, precision);
		if(debug)
		  wkt << w;
		nfaces++;
	      }
	    }
	  }
	  wkt.close();
	  std::cout << getTime() << "Done! " << nfaces << " faces saved." << std::endl;
	  break;
	}
      case 'a':
	{
	  string op = "SymmetricA";
	  std::cout << getTime() << op << std::endl;
	  // Go over the faces of the overlaid arrangement and their labels.
	  ArrangementRes_2::Face_iterator f;
	  ofstream wkt;
	  wkt.open (op + ".wkt");
	  std::cout << getTime() << "Saving faces to " + op + ".wkt..." << endl;
	  int nfaces = 0;
	  for (f = overlay_arr.faces_begin(); f != overlay_arr.faces_end(); ++f){
	    if(!f->is_unbounded()){
	      std::string data = f->data();
	      boost::char_separator<char> sep("|");
	      tokenizer tokens(data, sep);
	      char nlabels = 0;
	      for (tokenizer::iterator it = tokens.begin(); it != tokens.end(); ++it)
		nlabels++;
	      if(nlabels == 1 && data.find('A') != std::string::npos){
		std::string w = get_face_wkt<ArrangementRes_2> (f, precision);
		if(debug)
		  wkt << w;
		nfaces++;
	      }
	    }
	  }
	  wkt.close();
	  std::cout << getTime() << "Done! " << nfaces << " faces saved." << std::endl;
	  break;
	}
      case 'b':
	{
	  string op = "SymmetricB";
	  std::cout << getTime() << op << std::endl;
	  // Go over the faces of the overlaid arrangement and their labels.
	  ArrangementRes_2::Face_iterator f;
	  ofstream wkt;
	  wkt.open (op + ".wkt");
	  std::cout << getTime() << "Saving faces to " + op + ".wkt..." << endl;
	  int nfaces = 0;
	  for (f = overlay_arr.faces_begin(); f != overlay_arr.faces_end(); ++f){
	    if(!f->is_unbounded()){
	      std::string data = f->data();
	      boost::char_separator<char> sep("|");
	      tokenizer tokens(data, sep);
	      char nlabels = 0;
	      for (tokenizer::iterator it = tokens.begin(); it != tokens.end(); ++it)
		nlabels++;
	      if(nlabels == 1 && data.find('B') != std::string::npos){
		std::string w = get_face_wkt<ArrangementRes_2> (f, precision);
		if(debug)
		  wkt << w;
		nfaces++;
	      }
	    }
	  }
	  wkt.close();
	  std::cout << getTime() << "Done! " << nfaces << " faces saved." << std::endl;
	  break;
	}
      case 0:
	std::cout << getTime() << "No overlay operation has been selected." << std::endl;
	break;
      }
      milliSecondsElapsed = getMilliSpan(start);
      double timeOp = milliSecondsElapsed / 1000.0;
      std::cout << getTime() << "Time for overlay operator: " << timeOp << " s." << std::endl;
      std::cout << getTime() << "Total Time: " << timeA + timeB + timeO + timeOp << " s." << std::endl;
    }
    
  }
  
  return 0;
}
#else
int main()
{
  return 0;
}
#endif
