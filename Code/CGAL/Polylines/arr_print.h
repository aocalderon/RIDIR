#ifndef _PRINT_ARR_H_
#define _PRINT_ARR_H_

typedef CGAL::Exact_predicates_exact_constructions_kernel Kernel;
typedef CGAL::Arr_segment_traits_2<Kernel>                Segment_traits_2;
typedef CGAL::Arr_polyline_traits_2<Segment_traits_2>     Geom_traits_2;
typedef Geom_traits_2::Curve_2                            Polyline_2;

//-----------------------------------------------------------------------------
// Print all vertices in WKT format.
//
template<class Arrangement>
std::string get_wkt (typename Arrangement::Ccb_halfedge_const_circulator circ)
{
  typename Arrangement::Ccb_halfedge_const_circulator  curr = circ;
  typename Arrangement::Halfedge_const_handle          he;
  std::stringstream ss;

  ss << "POLYGON (("; //<< circ->source()->point();
  do {
    he = curr;
    int n = he->curve().number_of_subcurves();
    if( n > 1 ){
      Polyline_2 poly = he->curve();

      if(poly[0].left()  == he->source()->point() ||
	 poly[0].right() == he->source()->point()){
	for(int i = 0; i < n; i++){
	  ss << poly[i].source() << " " << poly[i].target() << ", ";
	}
      } else {
	for(int i = n - 1; i >= 0; i--){
	  ss << poly[i].target() << " " << poly[i].source() << ", ";
	}
      }
    } else {
      ss << he->source()->point() << " " << he->target()->point() <<", ";      
    }
    ++curr;
  } while (curr != circ);
  ss.seekp(-2, std::ios_base::end);
  ss << "))";

  return ss.str();
}

//-----------------------------------------------------------------------------
// Print the boundary description of an arrangement face in WKT format.
//
template<class Arrangement>
std::string get_face_wkt (typename Arrangement::Face_const_handle f)
{
  // Print the outer boundary.
  // + "\t" + f->data() 
  return get_wkt<Arrangement> (f->outer_ccb()) + "\n";
}

//-----------------------------------------------------------------------------
// Print all neighboring vertices to a given arrangement vertex.
//
template<class Arrangement>
void print_neighboring_vertices (typename Arrangement::Vertex_const_handle v)
{
  if (v->is_isolated())
  {
    std::cout << "The vertex (" << v->point() << ") is isolated" << std::endl;
    return;
  }

  typename Arrangement::Halfedge_around_vertex_const_circulator  first, curr;
  typename Arrangement::Vertex_const_handle                      u;

  std::cout << "The neighbors of the vertex (" << v->point() << ") are:";
  first = curr = v->incident_halfedges();
  do
  {
    // Note that the current halfedge is (u -> v):
    u = curr->source();
    std::cout << " (" << u->point() << ")";

    ++curr;
  } while (curr != first);
  std::cout << std::endl;

  return;
}

//-----------------------------------------------------------------------------
// Print all vertices (points) and edges (curves) along a connected component
// boundary.
//
template<class Arrangement>
void print_ccb (typename Arrangement::Ccb_halfedge_const_circulator circ)
{
  typename Arrangement::Ccb_halfedge_const_circulator  curr = circ;
  typename Arrangement::Halfedge_const_handle          he;

  std::cout << "(" << curr->source()->point() << ")";
  do
  {
    he = curr;
    std::cout << "   [" << he->curve() << "]   "
              << "(" << he->target()->point() << ")";

    ++curr;
  } while (curr != circ);
  std::cout << std::endl;

  return;
}

//-----------------------------------------------------------------------------
// Print the boundary description of an arrangement face.
//
template<class Arrangement>
void print_face (typename Arrangement::Face_const_handle f)
{
  // Print the outer boundary.
  if (f->is_unbounded())
  {
    std::cout << "Unbounded face. " << std::endl;
  }
  else
  {
    std::cout << "Outer boundary: ";
    print_ccb<Arrangement> (f->outer_ccb());
  }

  // Print the boundary of each of the holes.
  typename Arrangement::Hole_const_iterator  hole;
  int                                         index = 1;

  for (hole = f->holes_begin(); hole != f->holes_end(); ++hole, ++index)
  {
    std::cout << "    Hole #" << index << ": ";
    print_ccb<Arrangement> (*hole);
  }

  // Print the isolated vertices.
  typename Arrangement::Isolated_vertex_const_iterator  iv;

  for (iv = f->isolated_vertices_begin(), index = 1;
       iv != f->isolated_vertices_end(); ++iv, ++index)
  {
    std::cout << "    Isolated vertex #" << index << ": "
              << "(" << iv->point() << ")" << std::endl;
  }

  return;
}

//-----------------------------------------------------------------------------
// Print the given arrangement.
//
template<class Arrangement>
void print_arrangement (const Arrangement& arr)
{
  CGAL_precondition (arr.is_valid());

  // Print the arrangement vertices.
  typename Arrangement::Vertex_const_iterator  vit;

  std::cout << arr.number_of_vertices() << " vertices:" << std::endl;
  for (vit = arr.vertices_begin(); vit != arr.vertices_end(); ++vit)
  {
    std::cout << "(" << vit->point() << ")";
    if (vit->is_isolated())
      std::cout << " - Isolated." << std::endl;
    else
      std::cout << " - degree " << vit->degree() << std::endl;
  }

  // Print the arrangement edges.
  typename Arrangement::Edge_const_iterator    eit;

  std::cout << arr.number_of_edges() << " edges:" << std::endl;
  for (eit = arr.edges_begin(); eit != arr.edges_end(); ++eit)
    std::cout << "[" << eit->curve() << "]" << std::endl;

  // Print the arrangement faces.
  typename Arrangement::Face_const_iterator    fit;

  std::cout << arr.number_of_faces() << " faces:" << std::endl;
  for (fit = arr.faces_begin(); fit != arr.faces_end(); ++fit)
    print_face<Arrangement> (fit);

  return;
}

#endif
