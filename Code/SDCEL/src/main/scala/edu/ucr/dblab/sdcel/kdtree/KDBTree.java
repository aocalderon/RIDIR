/*
 * FILE: KDBTree
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.ucr.dblab.sdcel.kdtree;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import edu.ucr.dblab.sdcel.quadtree.StandardQuadTree;

import java.io.Serializable;
import java.util.*;

/**
 * see https://en.wikipedia.org/wiki/K-D-B-tree
 */
public class KDBTree implements Serializable {
    private final int maxItemsPerNode;
    private final int maxLevels;
    private final Envelope extent;
    private final int level;
    private final List<Envelope> items = new ArrayList<>();
    private KDBTree[] children;
    private int leafId = -1;
    public String lineage = "";
    public int splitX = -1;

    public KDBTree(int maxItemsPerNode, int maxLevels, Envelope extent) {
        this(maxItemsPerNode, maxLevels, 0, extent);
    }

    private KDBTree(int maxItemsPerNode, int maxLevels, int level, Envelope extent) {
        this.maxItemsPerNode = maxItemsPerNode;
        this.maxLevels = maxLevels;
        this.level = level;
        this.extent = extent;
    }

    public int getItemCount() {
        return items.size();
    }

    public List<Envelope> getItems() {
        return items;
    }

    public boolean isLeaf() {
        return children == null;
    }

    public int getLeafId() {
        if (!isLeaf()) {
            //throw new IllegalStateException();
            return -1;
        }

        return leafId;
    }

    public Envelope getExtent() {
        return extent;
    }

    public void insert(Envelope envelope) {
        if (items.size() < maxItemsPerNode || level >= maxLevels) {
            items.add(envelope);
        } else {
            if (children == null) {
                // Split over longer side
                double  width = extent.getWidth();
                double height = extent.getHeight();
                boolean splitX = width > height;
                boolean ok = split(splitX);
                if (!ok) {
                    // Try spitting by the other side
                    ok = split(!splitX);
                }

                if (!ok) {
                    // This could happen if all envelopes are the same.
                    items.add(envelope);
                    return;
                }
            }

            for (KDBTree child : children) {
                if (child.extent.contains(envelope.getMinX(), envelope.getMinY())) {
                    child.insert(envelope);
                    break;
                }
            }
        }
    }

    public void dropElements() {
        traverse(new Visitor() {
            @Override
            public boolean visit(KDBTree tree) {
                tree.items.clear();
                return true;
            }
        });
    }

    public Map<Integer, Envelope> getLeaves(){
        final Map<Integer, Envelope> matches = new HashMap<>();
        traverse(new Visitor() {
            @Override
            public boolean visit(KDBTree tree) {
                if (tree.isLeaf()) {
                    matches.put(tree.getLeafId(), tree.getExtent());
                }
                return true;
            }
        });
        return matches;
    }

    public Map<Integer, Polygon> getCells(GeometryFactory G){
        final Map<Integer, Polygon> matches = new HashMap<>();
        traverse(new Visitor() {
            @Override
            public boolean visit(KDBTree tree) {
                if (tree.isLeaf()) {
                    int id = tree.getLeafId();
                    Polygon mbr = (Polygon) G.toGeometry(tree.getExtent());
                    mbr.setUserData(id + "\t" + tree.lineage + "\t" + tree.splitX);
                    matches.put(id, mbr);
                }
                return true;
            }
        });
        return matches;
    }

    public List<KDBTree> findLeafNodes(final Envelope envelope) {
        final List<KDBTree> matches = new ArrayList<>();
        traverse(new Visitor() {
            @Override
            public boolean visit(KDBTree tree) {
                if (!disjoint(tree.getExtent(), envelope)) {
                    if (tree.isLeaf()) {
                        matches.add(tree);
                    }
                    return true;
                } else {
                    return false;
                }
            }
        });

        return matches;
    }

    private boolean disjoint(Envelope r1, Envelope r2) {
        return !r1.intersects(r2) && !r1.covers(r2) && !r2.covers(r1);
    }

    public interface Visitor {
        /**
         * Visits a single node of the tree
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(KDBTree tree);
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     */
    public void traverse(Visitor visitor) {
        if (!visitor.visit(this)) {
            return;
        }

        if (children != null) {
            for (KDBTree child : children) {
                child.traverse(visitor);
            }
        }
    }

    private interface VisitorWithLineage
    {
        /**
         * Visits a single node of the tree, with the traversal trace
         *
         * @param tree Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(KDBTree tree, String lineage);
    }
    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to Visitor.visit returns false.
     * lineage will memorize the traversal path for each nodes
     */
    private void traverseWithTrace(KDBTree.VisitorWithLineage visitor, String lineage)
    {
        if (!visitor.visit(this, lineage)) {
            return;
        }

        if (children != null) {
            children[0].traverseWithTrace(visitor, lineage+0);
            children[1].traverseWithTrace(visitor, lineage+1);
        }
    }

    public void assignPartitionLineage()
    {
        traverseWithTrace(new VisitorWithLineage()
        {
            @Override
            public boolean visit(KDBTree tree, String lineage)
            {
                //if (tree.isLeaf()) {
                    tree.lineage = lineage;
                //}
                return true;
            }
        }, "");
    }


    public void assignLeafIds() {
        traverse(new Visitor() {
            int id = 0;

            @Override
            public boolean visit(KDBTree tree) {
                if (tree.isLeaf()) {
                    tree.leafId = id;
                    id++;
                }
                return true;
            }
        });
    }

    private boolean split(boolean splitX) {
        final Comparator<Envelope> comparator = splitX ? new XComparator() : new YComparator();
        Collections.sort(items, comparator);

        final Envelope[] splits;
        final Splitter splitter;
        int middle = (int) Math.floor(items.size() / 2);
        Envelope middleItem = items.get(middle);
        int spx = -1;
        if (splitX) {
            double x = middleItem.centre().x;
            if (x > extent.getMinX() && x < extent.getMaxX()) {
                spx = 1;
                splits = splitAtX(extent, x);
                splitter = new XSplitter(x);
            } else {
                // Too many objects are crowded at the edge of the extent. Can't split.
                return false;
            }
        } else {
            double y = middleItem.centre().y;
            if (y > extent.getMinY() && y < extent.getMaxY()) {
                spx = 0;
                splits = splitAtY(extent, y);
                splitter = new YSplitter(y);
            } else {
                // Too many objects are crowded at the edge of the extent. Can't split.
                return false;
            }
        }

        children = new KDBTree[2];
        children[0] = new KDBTree(maxItemsPerNode, maxLevels, level + 1, splits[0]);
        children[0].splitX = spx;
        children[1] = new KDBTree(maxItemsPerNode, maxLevels, level + 1, splits[1]);
        children[1].splitX = spx;

        // Move items
        splitItems(splitter);
        return true;
    }

    private static final class XComparator implements Comparator<Envelope> {
        @Override
        public int compare(Envelope o1, Envelope o2) {
            double deltaX = o1.getMinX() - o2.getMinX();
            return (int) Math.signum(deltaX != 0 ? deltaX : o1.getMinY() - o2.getMinY());
        }
    }

    private static final class YComparator implements Comparator<Envelope> {
        @Override
        public int compare(Envelope o1, Envelope o2) {
            double deltaY = o1.getMinY() - o2.getMinY();
            return (int) Math.signum(deltaY != 0 ? deltaY : o1.getMinX() - o2.getMinX());
        }
    }

    private interface Splitter {
        /**
         * @return true if the specified envelope belongs to the lower split
         */
        boolean split(Envelope envelope);
    }

    private static final class XSplitter implements Splitter {
        private final double x;

        private XSplitter(double x) {
            this.x = x;
        }

        @Override
        public boolean split(Envelope envelope) {
            return envelope.getMinX() <= x;
        }
    }

    private static final class YSplitter implements Splitter {
        private final double y;

        private YSplitter(double y) {
            this.y = y;
        }

        @Override
        public boolean split(Envelope envelope) {
            return envelope.getMinY() <= y;
        }
    }

    private void splitItems(Splitter splitter) {
        for (Envelope item : items) {
            children[splitter.split(item) ? 0 : 1].insert(item);
        }
    }

    private Envelope[] splitAtX(Envelope envelope, double x) {
        assert (envelope.getMinX() < x);
        assert (envelope.getMaxX() > x);
        Envelope[] splits = new Envelope[2];
        splits[0] = new Envelope(envelope.getMinX(), x, envelope.getMinY(), envelope.getMaxY());
        splits[1] = new Envelope(x, envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY());
        return splits;
    }

    private Envelope[] splitAtY(Envelope envelope, double y) {
        assert (envelope.getMinY() < y);
        assert (envelope.getMaxY() > y);
        Envelope[] splits = new Envelope[2];
        splits[0] = new Envelope(envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), y);
        splits[1] = new Envelope(envelope.getMinX(), envelope.getMaxX(), y, envelope.getMaxY());
        return splits;
    }
}
