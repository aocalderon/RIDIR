#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <iostream>

using namespace std;
namespace bg = boost::geometry;

int main()
{
  typedef bg::model::d2::point_xy<double> point_type;
  bg::model::polygon<point_type> poly;
  bg::read_wkt("POLYGON((0 0,0 7,4 2,2 0,0 0))", poly);

  //getting the vertices back
  for(auto it = boost::begin(bg::exterior_ring(poly)); it != boost::end(bg::exterior_ring(poly)); ++it)
    {
      double x = bg::get<0>(*it);
      double y = bg::get<1>(*it);
      //use the coordinates...
      cout << "(" << x << ", " << y << ")"<< endl;
    }    

  cout << bg::num_points(poly) << endl;

  return 0;
}
