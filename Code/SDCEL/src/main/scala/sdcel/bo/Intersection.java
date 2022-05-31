package edu.ucr.dblab.bo;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import java.util.ArrayList;

public class Intersection {
    private Point p;
    private Segment seg1;
    private Segment seg2;

    public Intersection(Point p, Segment seg1, Segment seg2) {
        this.p    = p;
        this.seg1 = seg1;
        this.seg2 = seg2;
    }

    public Point getPoint() {
        return this.p;
    }

    public ArrayList<Segment> getSegments() {
	ArrayList<Segment> segs = new ArrayList<>(2);
	segs.add(this.seg1);
	segs.add(this.seg2);
	return segs;
    }

    public Coordinate getCoordinate() {
	return this.p.asJTSCoordinate();
    }

    public ArrayList<LineString> getLines() {
	ArrayList<LineString> lines = new ArrayList<>(2);
	lines.add(this.seg1.asJTSLine());
	lines.add(this.seg2.asJTSLine());
	return lines;
    }

    @Override
    public String toString() {
	return "" + p + "\t" + seg1 + "\t" + seg2;
    }
}
