package edu.ucr.dblab.bo;

import static java.lang.Math.sqrt;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * Created by valen_000 on 14. 5. 2017.
 */

public class Point {
    private double x_coord;
    private double y_coord;

    public Point(double x, double y) {
        this.x_coord = x;
        this.y_coord = y;
    }

    public Point(Coordinate c) {
        this.x_coord = c.x;
        this.y_coord = c.y;
    }

    public double get_x_coord() {
        return this.x_coord;
    }

    public void set_x_coord(double x_coord) {
        this.x_coord = x_coord;
    }

    public double get_y_coord() {
        return this.y_coord;
    }

    public void set_y_coord(double y_coord) {
        this.y_coord = y_coord;
    }

    public double distance(Point that){
	double dx = this.x_coord - that.x_coord;
	double dy = this.y_coord - that.x_coord;

	return sqrt( (dx * dx) + (dy * dy) );
    }

    public Coordinate asJTSCoordinate() {
	return new Coordinate(this.x_coord, this.y_coord);
    }

    @Override
    public String toString() {
	return "POINT (" + x_coord + " " + y_coord + ")";
    }
}