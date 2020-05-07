//! \file examples/Arrangement_on_surface_2/polylines.cpp
// Constructing an arrangement of polylines.

#include <CGAL/Exact_predicates_exact_constructions_kernel.h>
#include <CGAL/Arr_segment_traits_2.h>
#include <CGAL/Arr_polyline_traits_2.h>
#include <CGAL/Arrangement_2.h>
#include <vector>
#include <list>

#include "arr_print.h"
/*
  Define the Arrangement traits class to be used. You can either use some user
  defined kernel and Segment_traits_2 or the defaults.
 */

// Instantiate the traits class using a user-defined kernel
// and Segment_traits_2.
typedef CGAL::Exact_predicates_exact_constructions_kernel Kernel;
typedef CGAL::Arr_segment_traits_2<Kernel>                Segment_traits_2;
typedef CGAL::Arr_polyline_traits_2<Segment_traits_2>     Geom_traits_2;

// Identical instantiation can be achieved using the default Kernel:
// typedef CGAL::Arr_polyline_traits_2<>                    Geom_traits_2;


typedef Geom_traits_2::Point_2                            Point_2;
typedef Geom_traits_2::Segment_2                          Segment_2;
typedef Geom_traits_2::Curve_2                            Polyline_2;
typedef CGAL::Arrangement_2<Geom_traits_2>                Arrangement_2;

int main()
{
  Geom_traits_2 traits;
  Arrangement_2 arr(&traits);

  Geom_traits_2::Construct_curve_2 polyline_construct =
    traits.construct_curve_2_object();

  std::vector<Segment_2> segs;
  segs.push_back(Segment_2(Point_2(0, 2), Point_2(1, 2)));
  segs.push_back(Segment_2(Point_2(1, 2), Point_2(3, 6)));
  segs.push_back(Segment_2(Point_2(3, 6), Point_2(5, 2)));
  Polyline_2 lines = polyline_construct(segs.begin(), segs.end());

  insert(arr, lines);

  print_arrangement(arr);
  std::cout << "The arrangement size:" << std::endl
            << "   V = " << arr.number_of_vertices()
            << ",  E = " << arr.number_of_edges() 
            << ",  F = " << arr.number_of_faces() << std::endl;
  
  return 0;
}
