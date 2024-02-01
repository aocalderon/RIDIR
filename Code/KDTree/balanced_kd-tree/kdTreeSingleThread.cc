/*
 * Copyright (c) 2015, Russell A. Brown
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSEARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* @(#)kdTreeSingleThread.cc	1.65 04/25/15 */

/*
 * The k-d tree was described by Jon Bentley in "Multidimensional Binary Search Trees
 * Used for Associative Searching", CACM 18(9): 509-517, 1975.  For k dimensions and
 * n elements of data, a balanced k-d tree is built in O(kn log n) + O((k+1)n log n)
 * time by first sorting the data in each of k dimensions, then building the k-d tree
 * in a manner that preserves the order of the k sorts while recursively partitioning
 * the data at each level of the k-d tree. No further sorting is necessary.  Moreover,
 * it is possible to replace the O((k+1)n log n) term with a O((k-1)n log n) term but
 * this approach sacrifices the generality of building the k-d tree for points of any
 * number of dimensions.
 */

#include <stdbool.h>
#include <stdlib.h>
#include <vector>
#include <list>
#include <iostream>

using namespace std;

/* One node of a k-d tree */
class KdNode
{
private:
    const long *tuple;
    const KdNode *ltChild, *gtChild;

public:
    KdNode(const long *t)
    {
	this->tuple = t;
	this->ltChild = this->gtChild = NULL;
    }

public:
    const long *getTuple() const
    {
	return this->tuple;
    }

    /*
     * Initialize a reference array by creating references into the coordinates array.
     *
     * calling parameters:
     *
     * coordinates - a vector<long*> of pointers to each of the (x, y, z, w...) tuples
     * reference - a vector<long*> that represents one of the reference arrays
     */
private:
    static void initializeReference(vector<long*>& coordinates, vector<long*>& reference)
    {
	for (long j = 0; j < coordinates.size(); j++) {
	    reference.at(j) = coordinates.at(j);
	}
    }

    /*
     * The superKeyCompare method compares two long arrays in all k dimensions,
     * and uses the sorting or partition coordinate as the most significant dimension.
     *
     * calling parameters:
     *
     * a - a long*
     * b - a long*
     * p - the most significant dimension
     * dim - the number of dimensions
     *
     * returns: a long result of comparing two long arrays
     */
private:
    static long superKeyCompare(const long *a, const long *b, const long p, const long dim)
    {
	long diff = 0;
	for (long i = 0; i < dim; i++) {
	    long r = i + p;
	    // A fast alternative to the modulus operator for (i + p) < 2 * dim.
	    r = (r < dim) ? r : r - dim;
	    diff = a[r] - b[r];
	    if (diff != 0) {
		break;
	    }
	}
	return diff;
    }

    /*
     * The mergeSort function recursively subdivides the array to be sorted
     * then merges the elements. Adapted from Robert Sedgewick's "Algorithms
     * in C++" p. 166. Addison-Wesley, Reading, MA, 1992.
     *
     * calling parameters:
     *
     * reference - a vector<long*> that represents the reference array to sort
     * temporary - a temporary array into which to copy intermediate results;
     *             this array must be as large as the reference array
     * low - the start index of the region of the reference array to sort
     * high - the high index of the region of the reference array to sort
     * p - the sorting partition (x, y, z, w...)
     * dim - the number of dimensions
     * depth - the depth of subdivision
     */
private:
    static void mergeSort(vector<long*> &reference, vector<long*>& temporary, const long low, const long high,
			  const long p, const long dim)
    {
	long i, j, k;

	if (high > low) {

	    // Avoid overflow when calculating the median.
	    const long mid = low + ( (high - low) >> 1 );

	    // Recursively subdivide the lower and upper halves of the array.
	    mergeSort(reference, temporary, low, mid, p, dim);
	    mergeSort(reference, temporary, mid+1, high, p, dim);


	    // Merge the results for this level of subdivision.
	    for (i = mid+1; i > low; i--) {
		temporary.at(i-1) = reference.at(i-1);
	    }
	    for (j = mid; j < high; j++) {
		temporary.at(mid+(high-j)) = reference.at(j+1); // Avoid address overflow.
	    }
	    for (k = low; k <= high; k++) {
		reference.at(k) =
		    (superKeyCompare(temporary.at(i), temporary.at(j), p, dim) < 0) ? temporary.at(i++) : temporary.at(j--);
	    }
	}
    }

    /*
     * Check the validity of the merge sort and remove duplicates from a reference array.
     *
     * calling parameters:
     *
     * reference - a vector<long*> that represents one of the reference arrays
     * i - the leading dimension for the super key
     * dim - the number of dimensions
     *
     * returns: the end index of the reference array following removal of duplicate elements
     */
private:
    static long removeDuplicates(vector<long*>& reference, const long i, const long dim)
    {
	long end = 0;
	for (long j = 1; j < reference.size(); j++) {
	    long compare = superKeyCompare(reference.at(j), reference.at(j-1), i, dim);
	    if (compare < 0) {
		cout << "merge sort failure: superKeyCompare(ref[" << j << "], ref["
		     << j-1 << "], (" << i << ") = " << compare  << endl;
		exit(1);
	    } else if (compare > 0) {
		reference.at(++end) = reference.at(j);
	    }
	}
	return end;
    }

    /*
     * This function builds a k-d tree by recursively partitioning the
     * reference arrays and adding kdNodes to the tree.  These arrays
     * are permuted cyclically for successive levels of the tree in
     * order that sorting occur on x, y, z, w...
     *
     * calling parameters:
     *
     * references - a vector< vector<long*> > of pointers to each of the (x, y, z, w...) tuples
     * temporary - a vector<long*> that is used as a temporary array
     * start - start element of the reference arrays
     * end - end element of the reference arrays
     * dim - the number of dimensions
     * depth - the depth in the tree
     *
     * returns: a KdNode pointer to the root of the k-d tree
     */
private:
    static KdNode *buildKdTree(vector< vector<long*> >& references, vector<long*>& temporary, const long start,
			       const long end, const long dim, const long depth)
    {
	KdNode *node;

	// The axis permutes as x, y, z, w... and addresses the referenced data.
	long axis = depth % dim;

	if (end == start) {

	    // Only one reference was passed to this function, so add it to the tree.
	    node = new KdNode( references.at(0).at(end) );

	} else if (end == start + 1) {

	    // Two references were passed to this function in sorted order, so store the start
	    // element at this level of the tree and store the end element as the > child.
	    node = new KdNode( references.at(0).at(start) );
	    node->gtChild = new KdNode( references.at(0).at(end) );

	} else if (end == start + 2) {

	    // Three references were passed to this function in sorted order, so
	    // store the median element at this level of the tree, store the start
	    // element as the < child and store the end element as the > child.
	    node = new KdNode( references.at(0).at(start + 1) );
	    node->ltChild = new KdNode( references.at(0).at(start) );
	    node->gtChild = new KdNode( references.at(0).at(end) );

	} else if (end > start + 2) {

	    // More than three references were passed to this function, so
	    // the median element of references[0] is chosen as the tuple about
	    // which the other reference arrays will be partitioned.  Avoid
	    // overflow when computing the median.
	    const long median = start + ((end - start) / 2);

	    // Store the median element of references[0] in a new kdNode.
	    node = new KdNode( references.at(0).at(median) );

	    // Copy references[0] to the temporary array before partitioning.
	    for (long i = start; i <= end; i++) {
		temporary.at(i) = references.at(0).at(i);
	    }

	    // Process each of the other reference arrays in a priori sorted order
	    // and partition it by comparing super keys.  Store the result from
	    // references[i] in references[i-1], thus permuting the reference
	    // arrays.  Skip the element of references[i] that that references
	    // a point that equals the point that is stored in the new k-d node.
	    long lower, upper, lowerSave, upperSave;
	    for (long i = 1; i < dim; i++) {

		// Process one reference array.  Compare once only.
		lower = start - 1;
		upper = median;
		for (long j = start; j <= end; j++) {
		    long compare = superKeyCompare(references.at(i).at(j), node->tuple, axis, dim);
		    if (compare < 0) {
			references.at(i-1).at(++lower) = references.at(i).at(j);
		    } else if (compare > 0) {
			references.at(i-1).at(++upper) = references.at(i).at(j);
		    }
		}

		// Check the new indices for the reference array.
		if (lower < start || lower >= median) {
		    cout << "incorrect range for lower at depth = " << depth << " : start = "
			 << start << "  lower = " << lower << "  median = " << median << endl;
		    exit(1);
		}

		if (upper <= median || upper > end) {
		    cout << "incorrect range for upper at depth = " << depth << " : median = "
			 << median << "  upper = " << upper << "  end = " << end << endl;
		    exit(1);
		}

		if (i > 1 && lower != lowerSave) {
		    cout << " lower = " << lower << "  !=  lowerSave = " << lowerSave << endl;
		    exit(1);
		}

		if (i > 1 && upper != upperSave) {
		    cout << " upper = " << upper << "  !=  upperSave = " << upperSave << endl;
		    exit(1);
		}

		lowerSave = lower;
		upperSave = upper;
	    }

	    // Copy the temporary array to references[dim-1] to finish permutation.
	    for (long i = start; i <= end; i++) {
		references.at(dim - 1).at(i) = temporary.at(i);
	    }

	    // Recursively build the < branch of the tree.
	    node->ltChild = buildKdTree(references, temporary, start, lower, dim, depth+1);

	    // Recursively build the > branch of the tree.
	    node->gtChild = buildKdTree(references, temporary, median+1, upper, dim, depth+1);

	} else if (end < start) {

	    // This is an illegal condition that should never occur, so test for it last.
	    cout << "error has occurred at depth = " << depth << " : end = " << end
		 << "  <  start = " << start << endl;
	    exit(1);

	}

	// Return the pointer to the root of the k-d tree.
	return node;
    }

    /*
     * Walk the k-d tree and check that the children of a node are in the correct branch of that node.
     *
     * calling parameters:
     *
     * dim - the number of dimensions
     * depth - the depth in the k-d tree 
     *
     * returns: a count of the number of kdNodes in the k-d tree
     */
private:
    long verifyKdTree(const long dim, const long depth) const
    {
	long count = 1 ;
	if (this->tuple == NULL) {
	    cout << "point is null!" << endl;
	    exit(1);
	}

	// The partition cycles as x, y, z, w...
	long axis = depth % dim;

	if (this->ltChild != NULL) {
	    if (this->ltChild->tuple[axis] > this->tuple[axis]) {
		cout << "child is > node!" << endl;
		exit(1);
	    }
	    if (superKeyCompare(this->ltChild->tuple, this->tuple, axis, dim) >= 0) {
		cout << "child is >= node!" << endl;
		exit(1);
	    }
	    count += this->ltChild->verifyKdTree(dim, depth + 1);
	}
	if (this->gtChild != NULL) {
	    if (this->gtChild->tuple[axis] < this->tuple[axis]) {
		cout << "child is < node!" << endl;
		exit(1);
	    }
	    if (superKeyCompare(this->gtChild->tuple, this->tuple, axis, dim) <= 0) {
		cout << "child is <= node" << endl;
		exit(1);
	    }
	    count += this->gtChild->verifyKdTree(dim, depth + 1);
	}
	return count;
    }

    /*
     * The createKdTree function performs the necessary initialization then calls the buildKdTree function.
     *
     * calling parameters:
     *
     * coordinates - a vector<long*> of references to each of the (x, y, z, w...) tuples
     * numDimensions - the number of dimensions
     *
     * returns: a KdNode pointer to the root of the k-d tree
     */
public:
    static KdNode *createKdTree(vector<long*>& coordinates, const long numDimensions)
    {
	// Initialize and sort the reference arrays.
	vector< vector<long*> > references(numDimensions, vector<long*>( coordinates.size() ) );
	vector<long*> temporary( coordinates.size() );;
	for (long i = 0; i < references.size(); i++) {
	    initializeReference(coordinates, references.at(i));
	    mergeSort(references.at(i), temporary, 0, references.at(i).size()-1, i, numDimensions);
	}

	// Remove references to duplicate coordinates via one pass through each reference array.
	vector<long> end( references.size() );
	for (long i = 0; i < end.size(); i++) {
	    end.at(i) = removeDuplicates(references.at(i), i, numDimensions);
	}

	// Check that the same number of references was removed from each reference array.
	for (long i = 0; i < end.size()-1; i++) {
	    for (long j = i + 1; j < end.size(); j++) {
		if ( end.at(i) != end.at(j) ) {
		    cout << "reference removal error" << endl;
		    exit(1);
		}
	    }
	}

	// Build the k-d tree.
	KdNode *root = buildKdTree(references, temporary, 0, end.at(0), numDimensions, 0);

	// Verify the k-d tree and report the number of KdNodes.
	long numberOfNodes = root->verifyKdTree(numDimensions, 0);
	cout << endl << "Number of nodes = " << numberOfNodes << endl;

	// Return the pointer to the root of the k-d tree.
	return root;
    }

    /*
     * Search the k-d tree and find the KdNodes that lie within a cutoff distance
     * from a query node in all k dimensions.
     *
     * calling parameters:
     *
     * query - the query point
     * cut - the cutoff distance
     * dim - the number of dimensions
     * depth - the depth in the k-d tree
     *
     * returns: a list that contains the kdNodes that lie within the cutoff distance of the query node
     */
public:
    list<KdNode> searchKdTree(const long* query, const long cut, const long dim,
				     const long depth) const {

	// The partition cycles as x, y, z, w...
	long axis = depth % dim;

	// If the distance from the query node to the k-d node is within the cutoff distance
	// in all k dimensions, add the k-d node to a list.
	list<KdNode> result;
	bool inside = true;
	for (long i = 0; i < dim; i++) {
	    if (abs(query[i] - this->tuple[i]) > cut) {
		inside = false;
		break;
	    }
	}
	if (inside) {
	    result.push_back(*this); // The push_back function expects a KdNode for a call by reference.
	}

	// Search the < branch of the k-d tree if the partition coordinate of the query point minus
	// the cutoff distance is <= the partition coordinate of the k-d node.  The < branch must be
	// searched when the cutoff distance equals the partition coordinate because the super key
	// may assign a point to either branch of the tree if the sorting or partition coordinate,
	// which forms the most significant portion of the super key, shows equality.
	if ( this->ltChild != NULL && (query[axis] - cut) <= this->tuple[axis] ) {
	    list<KdNode> ltResult = this->ltChild->searchKdTree(query, cut, dim, depth + 1);
	    result.splice(result.end(), ltResult); // Can't substitute searchKdTree(...) for ltResult.
	}

	// Search the > branch of the k-d tree if the partition coordinate of the query point plus
	// the cutoff distance is >= the partition coordinate of the k-d node.  The < branch must be
	// searched when the cutoff distance equals the partition coordinate because the super key
	// may assign a point to either branch of the tree if the sorting or partition coordinate,
	// which forms the most significant portion of the super key, shows equality.
	if ( this->gtChild != NULL && (query[axis] + cut) >= this->tuple[axis] ) {
	    list<KdNode> gtResult = this->gtChild->searchKdTree(query, cut, dim, depth + 1);
	    result.splice(result.end(), gtResult); // Can't substitute searchKdTree(...) for gtResult.
	}

	return result;
    }

    /*
     * Print one tuple.
     *
     * calling parameters:
     *
     * tuple - the tuple to print
     * dim - the number of dimensions
     */
public:
    static void printTuple(const long* tuple, const long dim)
    {
	cout << "(" << tuple[0] << ",";
	for (long i=1; i<dim-1; i++) cout << tuple[i] << ",";
	cout << tuple[dim-1] << ")";
    }

    /*
     * Print the k-d tree "sideways" with the root at the ltChild.
     *
     * calling parameters:
     *
     * dim - the number of dimensions
     * depth - the depth in the k-d tree 
     */
public:
    void printKdTree(const long dim, const long depth) const
    {
	if (this->gtChild != NULL) {
	    this->gtChild->printKdTree(dim, depth+1);
	}
	for (long i=0; i<depth; i++) cout << "       ";
	printTuple(this->tuple, dim);
	cout << endl;
	if (this->ltChild != NULL) {
	    this->ltChild->printKdTree(dim, depth+1);
	}
    }
};

#define DIMENSIONS (3)
#define NUM_TUPLES (25)
#define SEARCH_DISTANCE (2)

/* Create a simple k-d tree and print its topology for inspection. */
int main(int argc, char **argv)
{
    // Declare the two-dimensional coordinates array that contains (x,y,z) coordinates.
    long coordinates[NUM_TUPLES][DIMENSIONS] = {
	{2,3,3}, {5,4,2}, {9,6,7}, {4,7,9}, {8,1,5},
	{7,2,6}, {9,4,1}, {8,4,2}, {9,7,8}, {6,3,1},
	{3,4,5}, {1,6,8}, {9,5,3}, {2,1,3}, {8,7,6},
	{5,4,2}, {6,3,1}, {8,7,6}, {9,6,7}, {2,1,3},
	{7,2,6}, {4,7,9}, {1,6,8}, {3,4,5}, {9,4,1} };

    // Create the k-d tree.  The two-dimensional array is indexed by
    // a vector<long*> in order to pass it as a function argument.
    // The array is not copied to a vector< vector<long> > because,
    // for efficiency, assignments in the initializeReference,
    // mergeSort and buildKdTree functions copy only the long*
    // pointer instead of all elements of a vector<long>.
    vector<long*> coordinateVector(NUM_TUPLES);
    for (long i = 0; i < coordinateVector.size(); i++) {
	coordinateVector.at(i) = &(coordinates[i][0]);
    }
    KdNode *root = KdNode::createKdTree(coordinateVector, DIMENSIONS);

    // Print the k-d tree "sideways" with the root at the left.
    cout << endl;
    root->printKdTree(DIMENSIONS, 0);

    // Search the k-d tree for the k-d nodes that lie within the cutoff distance.
    long query[3] = {4, 3, 1};
    list<KdNode> kdList = root->searchKdTree(query, SEARCH_DISTANCE, DIMENSIONS, 0);
    cout << endl << kdList.size() << " nodes within " << SEARCH_DISTANCE << " units of ";
    KdNode::printTuple(query, DIMENSIONS);
    cout << " in all dimensions." << endl << endl;
    if (kdList.size() != 0) {
    	cout << "List of k-d nodes within " << SEARCH_DISTANCE << "-unit search distance follows:" << endl << endl;
	list<KdNode>::iterator it;
	for (it = kdList.begin(); it != kdList.end(); it++) {
	    KdNode::printTuple(it->getTuple(), DIMENSIONS);
	    cout << " ";
    	}
    	cout << endl << endl;
    }	
    return 0;
}
