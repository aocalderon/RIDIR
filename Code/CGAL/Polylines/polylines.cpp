//! \file examples/Arrangement_on_surface_2/polylines.cpp
// Constructing an arrangement of polylines.

#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Arr_segment_traits_2.h>
#include <CGAL/Arr_polyline_traits_2.h>
#include <CGAL/Arrangement_2.h>
#include <CGAL/IO/WKT.h>

#include <vector>
#include <fstream>
#include <iostream>
#include <string>

#include "arr_print.h"

// General definitions...
typedef CGAL::Exact_predicates_exact_constructions_kernel Kernel;
typedef CGAL::Arr_segment_traits_2<Kernel>                Segment_traits_2;
typedef CGAL::Arr_polyline_traits_2<Segment_traits_2>     Geom_traits_2;
typedef Geom_traits_2::Point_2                            Point_2;
typedef Geom_traits_2::Segment_2                          Segment_2;
typedef Geom_traits_2::Curve_2                            Polyline_2;
typedef CGAL::Arrangement_2<Geom_traits_2>                Arrangement_2;


int main(int argc, char* argv[]) {
  // WKT reader definitions...
  typedef CGAL::Polygon_with_holes_2<Kernel>                  Polygon;
  typedef CGAL::Polygon_2<Kernel>::Vertex_iterator            vertexIt;
  Geom_traits_2 traits;
  Arrangement_2 arr(&traits);

  Geom_traits_2::Construct_curve_2 polyline_construct =
    traits.construct_curve_2_object();

  std::ifstream A( argv[1] );
  std::vector<Polygon> polys;

  // Reading WKT file...
  do {
    Polygon p;
    CGAL::read_polygon_WKT(A, p);
    if(!p.outer_boundary().is_empty())
      polys.push_back(p);
  } while(A.good() && !A.eof());

  // Inserting as polyline...
  for(Polygon p : polys){
    std::vector<Point_2> points;
    for (vertexIt vi = p.outer_boundary().begin(); vi != p.outer_boundary().end(); ++vi){
      points.push_back(*vi);
    }
    points.push_back(points[0]);
    Polyline_2 lines = polyline_construct(points.begin(), points.end());
    insert(arr, lines);
    std::cout << "Polygon added. " << points.size() << " segments." << std::endl;
  }

  //print_arrangement(arr);
  std::cout << "The arrangement size:" << std::endl
            << "   V = " << arr.number_of_vertices()
            << ",  E = " << arr.number_of_edges() 
            << ",  F = " << arr.number_of_faces() << std::endl;

  // Go over the faces of the overlaid arrangement and their labels.
  Arrangement_2::Face_iterator  res_fit;

  std::ofstream wkt;
  wkt.open ("faces.wkt");
  std::cout << "The current faces are: " << std::endl;
  for (res_fit = arr.faces_begin(); res_fit != arr.faces_end(); ++res_fit){
    if(!res_fit->is_unbounded()){
      std::string w = get_face_wkt<Arrangement_2> (res_fit);
      //std::cout << w;
      wkt << w;
    }
  }
  wkt.close();

  
  return 0;
}
