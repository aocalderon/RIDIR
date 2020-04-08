#include <boost/config.hpp>
#include <boost/version.hpp>

#if BOOST_VERSION >= 105600 && (! defined(BOOST_GCC) || BOOST_GCC >= 40500)
#include <iostream>
#include <fstream>

#include <CGAL/IO/WKT.h>

#include <CGAL/Simple_cartesian.h>
#include <CGAL/Exact_predicates_exact_constructions_kernel.h>

#include <vector>

//typedef CGAL::Simple_cartesian<CGAL::Gmpq> Kernel;

typedef CGAL::Exact_predicates_exact_constructions_kernel Kernel;

using namespace std;

int main(int argc, char* argv[])
{
  typedef CGAL::Polygon_with_holes_2<Kernel> Polygon;
  typedef CGAL::Point_2<Kernel> Point;
  typedef CGAL::Polygon_2<Kernel>::Vertex_iterator vertexIt;
  typedef Polygon::Hole_iterator holeIt;

  {
    ifstream is( (argc>1) ? argv[1] : "polygons.wkt" );
    list<Polygon> polys;

    do {
      Polygon p;
      CGAL::read_polygon_WKT(is, p);
      if(!p.outer_boundary().is_empty())
	polys.push_back(p);
    } while(is.good() && !is.eof());
    
    for(Polygon p : polys){
      vector<Point> points;
      for (vertexIt vi = p.outer_boundary().begin(); vi != p.outer_boundary().end(); ++vi){
	points.push_back(*vi);
      }
      points.push_back(points[0]);

      int n=0;
      cout << "Outer: " << endl;
      for(int i = 0; i < points.size() - 1; ++i){
	cout << "Segment " << n++ <<  " (" << points[i] << ", " << points[i + 1] << ")" << endl;
      }
      cout << endl;
      
      if(p.has_holes()){
	cout << "Hole(s): " << endl;
	for (holeIt hit = p.holes_begin(); hit != p.holes_end(); ++hit){
	  points.clear();
	  for (vertexIt vi = (*hit).begin(); vi != (*hit).end(); ++vi){
	    points.push_back(*vi);
	  }
	  points.push_back(points[0]);
	  
	  for(int i = 0; i < points.size() - 1; ++i){
	    cout << "Segment " << n++ <<  " (" << points[i] << ", " << points[i + 1] << ")" << endl;
	  }
	  cout << endl;
	}
      }
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
