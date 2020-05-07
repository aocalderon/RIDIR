#include <CGAL/Cartesian.h>
#include <CGAL/Quotient.h>
#include <CGAL/MP_Float.h>
#include <CGAL/Arr_segment_traits_2.h>
#include <CGAL/Arrangement_2.h>
#include <CGAL/Arr_naive_point_location.h>
#include <CGAL/Arr_landmarks_point_location.h>
#include <CGAL/IO/WKT.h>

#include "arr_print.h"

typedef CGAL::Quotient<CGAL::MP_Float>                    Number_type;
typedef CGAL::Cartesian<Number_type>                      Kernel;
typedef CGAL::Arr_segment_traits_2<Kernel>                Traits_2;
typedef Traits_2::Point_2                                 Point_2;
typedef Traits_2::X_monotone_curve_2                      Segment_2;
typedef CGAL::Arrangement_2<Traits_2>                     Arrangement_2;
typedef CGAL::Arr_landmarks_point_location<Arrangement_2> Landmarks_pl;

int main (int argc, char* argv[]){
  // Setting variables...
  std::string filename = "";
  int n = 0;
  int c;
  while ((c = getopt (argc, argv, "n:f:")) != -1){
    switch (c){
      case 'f':
	if(optarg) filename = optarg;
        break;
      case 'n':
	if(optarg) n = std::atoi(optarg);
        break;
      case '?':
        return 1;
      default:
	abort ();
    }
  }
  typedef std::vector<Point_2> LineString;
  std::ifstream input(filename);
  Segment_2 segment;
  Arrangement_2 arr;

  // Reading edges...
  std::cout << "Reading edges..." << std::endl;
  Landmarks_pl pl (arr);
  pl.attach(arr);
  int i = 1;
  do {
    LineString line;
    CGAL::read_linestring_WKT(input, line);
    segment = Segment_2 (line[0], line[1]);
    // Feeding the segments...
    insert(arr, segment, pl);
    i++;
    std::cout << i << " edges have been inserted..." << std::endl;
  } while(input.good() && !input.eof() && i < n);
  input.close();
  std::cout << "Done! " << n << " edges has been read." << std::endl;
  
  // Printing arrangement info...
  std::cout << "Filename: " << filename << " N: " << n << std::endl;
  std::cout << "The arrangement size:" << std::endl
            << "   V = " << arr.number_of_vertices()
            << ",  E = " << arr.number_of_edges() 
            << ",  F = " << arr.number_of_faces() << std::endl;
  
  return 0;
}
