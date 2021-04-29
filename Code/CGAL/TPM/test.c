using namespace CGAL;
typedef Pm_dcel<Tpm_vertex_base, Tpm_halfedge_base, Tpm_face_base> Dcel;
typedef Topological_map<Dcel> Tpm;

void Topological_triangle() {
  Tpm t;
  Tpm::Face_handle uf = t.unbounded_face();
  Tpm::Halfedge_handle e1 = t.insert_in_face_interior(uf);
  Tpm::Halfedge_handle e2 = t.insert_from_vertex(e1);
  t.insert_at_vertices(e2, e1->twin());
}
