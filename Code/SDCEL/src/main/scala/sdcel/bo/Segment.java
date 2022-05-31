package edu.ucr.dblab.bo;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Created by valen_000 on 14. 5. 2017.
 */

public class Segment {

    private Point p_1;
    private Point p_2;
    double value;
    String label;
    long id;

    public Segment(Point p_1, Point p_2) {
        this.p_1 = p_1;
        this.p_2 = p_2;
        this.calculate_value(this.first().get_x_coord());
	this.id = -1;
    }

    // Double-check vertical and horizontal cases...
    public Point first() {
        if(p_1.get_x_coord() < p_2.get_x_coord()) {
            return p_1;
        } else if(p_1.get_x_coord() > p_2.get_x_coord()) {
            return p_2;
        } else {
	    if(p_1.get_y_coord() < p_2.get_y_coord()) {
		return p_1;
	    } else {
		return p_2;
	    }
	}
    }

    // Double-check vertical and horizontal cases...
    public Point second() {
        if(p_1.get_x_coord() < p_2.get_x_coord()) {
            return p_2;
        } else if(p_1.get_x_coord() > p_2.get_x_coord()) {
            return p_1;
        } else {
	    if(p_1.get_y_coord() < p_2.get_y_coord()) {
		return p_2;
	    } else {
		return p_1;
	    }
	}
    }

    public void calculate_value(double value) {
        double x1 = this.first().get_x_coord();
        double x2 = this.second().get_x_coord();
        double y1 = this.first().get_y_coord();
        double y2 = this.second().get_y_coord();

	double dx = x2 - x1; // TODO: Track Zero division...
	double dy = y2 - y1;

	double vx = value - x1;
	
	this.value = y1 + ( (dy / dx) * vx ); // TODO: NaN value does not seem to affect...
    }

    public void set_value(double value) {
        this.value = value;
    }

    public double get_value() {
        return this.value;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return this.id;
    }

    public com.vividsolutions.jts.geom.LineString asJTSLine() {
	GeometryFactory geofactory = new GeometryFactory(new PrecisionModel(1000.0));
	Coordinate p1 = this.p_1.asJTSCoordinate();
	Coordinate p2 = this.p_2.asJTSCoordinate();
	Coordinate[] arr = new Coordinate[2]; 
	arr[0] = p1;
	arr[1] = p2;
	LineString line = geofactory.createLineString(arr);
	line.setUserData(this.value + "\t" + this.label + "\t" + this.id);
	return line;
    }

    @Override
    public String toString() {
	return "" + this.asJTSLine().toText() + "\t" + label + "\t" + id + "\t" + value ;
    }
}
