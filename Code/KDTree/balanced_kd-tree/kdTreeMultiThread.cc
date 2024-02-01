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

/* @(#)kdTreeMultiThread.cc	1.39 05/07/15 */

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

/* Sun Studio compilation options are -lm -lmtmalloc -xopenmp -O4 */

#include <limits.h>
#include <math.h>
#include <time.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <vector>
#include <list>
#include <iostream>
#include <iomanip>

using namespace std;

/*
 * The following provides an alternate to clock_gettime(CLOCK_REALTIME, &time) for Mach.
 * See http://stackoverflow.com/questions/5167269/clock-gettime-alternative-in-mac-os-x
 */
#ifdef MACH
#include <mach/mach_time.h>

#define MACH_NANO (+1.0E-9)
#define MACH_GIGA UINT64_C(1000000000)

static double mach_timebase = 0.0;
static uint64_t mach_timestart = 0;

struct timespec mach_gettime(void) {
    // be more careful in a multithreaded environement
    if (!mach_timestart) {
        mach_timebase_info_data_t tb = { 0 };
        mach_timebase_info(&tb);
        mach_timebase = tb.numer;
        mach_timebase /= tb.denom;
        mach_timestart = mach_absolute_time();
    }
    struct timespec t;
    double diff = (mach_absolute_time() - mach_timestart) * mach_timebase;
    t.tv_sec = diff * MACH_NANO;
    t.tv_nsec = diff - (t.tv_sec * MACH_GIGA);
    return t;
}
#endif

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
	    reference[j] = coordinates[j];
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
     * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
     * depth - the depth of subdivision
     */
private:
    static void mergeSort(vector<long*> &reference, vector<long*>& temporary, const long low, const long high,
			  const long p, const long dim, const long maximumSubmitDepth, const long depth)
    {
	long i, j, k;

	if (high > low) {

	    // Avoid overflow when calculating the median.
	    const long mid = low + ( (high - low) >> 1 );

	    // Is a child thread available to subdivide the lower half of the array?
	    if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

		// No, so recursively subdivide the lower half of the array with the current thread.
		mergeSort(reference, temporary, low, mid, p, dim, maximumSubmitDepth, depth+1);

		// Then recursively subdivide the upper half of the array with the current thread.
		mergeSort(reference, temporary, mid+1, high, p, dim, maximumSubmitDepth, depth+1);

	    } else {

		// Yes, a child thread is available, so recursively subdivide the lower half of the array
		// with a child thread.  The low, mid, p, dim, maximumSubmitDepth and depth variables are
		// read only, so they may be shared among threads, or firstprivate which copies them for
		// each thread. The copy operations are not expensive because there are not many threads.
#pragma omp task shared(reference, temporary) firstprivate(low, mid, p, dim, maximumSubmitDepth, depth)
		mergeSort(reference, temporary, low, mid, p, dim, maximumSubmitDepth, depth+1);

		// And simultaneously subdivide the upper half of the array with another child thread.
#pragma omp task shared(reference, temporary) firstprivate(mid, high, p, dim, maximumSubmitDepth, depth)
		mergeSort(reference, temporary, mid+1, high, p, dim, maximumSubmitDepth, depth+1);

		// Wait for both child threads to finish execution.
#pragma omp taskwait
	    }

	    // Merge the results for this level of subdivision.
	    for (i = mid+1; i > low; i--) {
		temporary[i-1] = reference[i-1];
	    }
	    for (j = mid; j < high; j++) {
		temporary[mid+(high-j)] = reference[j+1]; // Avoid address overflow.
	    }
	    for (k = low; k <= high; k++) {
		reference[k] =
		    (superKeyCompare(temporary[i], temporary[j], p, dim) < 0) ? temporary[i++] : temporary[j--];
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
	    long compare = superKeyCompare(reference[j], reference[j-1], i, dim);
	    if (compare < 0) {
		cout << "merge sort failure: superKeyCompare(ref[" << j << "], ref["
		     << j-1 << "], (" << i << ") = " << compare  << endl;
		exit(1);
	    } else if (compare > 0) {
		reference[++end] = reference[j];
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
     * references - a vector< vector<long*> > of references to each of the (x, y, z, w...) tuples
     * temporary - a vector<long*> that is used as a temporary array
     * start - start element of the reference array
     * end - end element of the reference array
     * dim - the number of dimensions
     * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
     * depth - the depth in the tree
     *
     * returns: a KdNode pointer to the root of the k-d tree
     */
private:
    static KdNode *buildKdTree(vector< vector<long*> >& references, vector<long*>& temporary, const long start,
			       const long end, const long dim, const long maximumSubmitDepth, const long depth)
    {
	KdNode *node;

	// The axis permutes as x, y, z, w... and addresses the referenced data.
	long axis = depth % dim;

	if (end == start) {

	    // Only one reference was passed to this function, so add it to the tree.
	    node = new KdNode( references[0][end] );

	} else if (end == start + 1) {

	    // Two references were passed to this function in sorted order, so store the start
	    // element at this level of the tree and store the end element as the > child.
	    node = new KdNode( references[0][start] );
	    node->gtChild = new KdNode( references[0][end] );

	} else if (end == start + 2) {

	    // Three references were passed to this function in sorted order, so
	    // store the median element at this level of the tree, store the start
	    // element as the < child and store the end element as the > child.
	    node = new KdNode( references[0][start + 1] );
	    node->ltChild = new KdNode( references[0][start] );
	    node->gtChild = new KdNode( references[0][end] );

	} else if (end > start + 2) {

	    // More than three references were passed to this function, so
	    // the median element of references[0] is chosen as the tuple about
	    // which the other reference arrays will be partitioned.  Avoid
	    // overflow when computing the median.
	    const long median = start + ((end - start) / 2);

	    // Store the median element of references[0] in a new kdNode.
	    node = new KdNode( references[0][median] );

	    // Copy references[0] to the temporary array before partitioning.
	    for (long i = start; i <= end; i++) {
		temporary[i] = references[0][i];
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
		    long compare = superKeyCompare(references[i][j], node->tuple, axis, dim);
		    if (compare < 0) {
			references[i-1][++lower] = references[i][j];
		    } else if (compare > 0) {
			references[i-1][++upper] = references[i][j];
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
		references[dim - 1][i] = temporary[i];
	    }

	    // Build the < branch with a child thread at as many levels of the tree as possible.
	    // Create the child thread as high in the tree as possible for greater utilization.

	    // Is a child thread available to build the < branch?
	    if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

		// No, so recursively build the < branch of the tree with the current thread.
		node->ltChild = buildKdTree(references, temporary, start, lower, dim, maximumSubmitDepth, depth+1);

		// Then recursively build the > branch of the tree with the current thread.
		node->gtChild = buildKdTree(references, temporary, median+1, upper, dim, maximumSubmitDepth, depth+1);

	    } else {

		// Yes, a child thread is available, so recursively build the < branch with a child thread.
		// The ltChild, references and t variables are read and/or written, so they must be shared among
		// threads.  The start, lower, dim, maximumSubmitDepth and depth variables are read only,
		// so they may be shared among threads, or firstprivate which copies them for each thread.
		// The copy operations are not expensive because there are not many threads.
#pragma omp task shared(node, references, temporary) firstprivate(start, lower, dim, maximumSubmitDepth, depth)
		node->ltChild = buildKdTree(references, temporary, start, lower, dim, maximumSubmitDepth, depth+1);

		// And simultaneously, recursively build the > branch of the tree with another child thread.
#pragma omp task shared(node, references, temporary) firstprivate(median, upper, dim, maximumSubmitDepth, depth)
		node->gtChild = buildKdTree(references, temporary, median+1, upper, dim, maximumSubmitDepth, depth+1);

		// Wait for both child threads to finish execution.
#pragma omp taskwait
	    }

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
     * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
     * depth - the depth in the k-d tree 
     *
     * returns: a count of the number of kdNodes in the k-d tree
     */
private:
    long verifyKdTree(const long dim, const long maximumSubmitDepth, const long depth) const
{
    long count = 1 ;
    if (tuple == NULL) {
    	cout << "point is null!" << endl;
    	exit(1);
    }

    // The partition cycles as x, y, z, w...
    long axis = depth % dim;

    if (ltChild != NULL) {
    	if (ltChild->tuple[axis] > tuple[axis]) {
	    cout << "child is > node!" << endl;
	    exit(1);
	}
	if (superKeyCompare(ltChild->tuple, tuple, axis, dim) >= 0) {
	    cout << "child is >= node!" << endl;
	    exit(1);
	}
    }
    if (gtChild != NULL) {
    	if (gtChild->tuple[axis] < tuple[axis]) {
	    cout << "child is < node!" << endl;
	    exit(1);
	}
	if (superKeyCompare(gtChild->tuple, tuple, axis, dim) <= 0) {
	    cout << "child is <= node" << endl;
	    exit(1);
	}
    }

    // Search the < branch with a child thread at as many levels of the tree as possible.
    // Create the child thread as high in the tree as possible for greater utilization.

    // Is a child thread available to build the < branch?
    if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

	// No, so search the < branch with the current thread.
	if (ltChild != NULL) {
	    count += ltChild->verifyKdTree(dim, maximumSubmitDepth, depth + 1);
	}

	// Then search the > branch with the current thread.
	if (gtChild != NULL) {
	    count += gtChild->verifyKdTree(dim, maximumSubmitDepth, depth + 1);
	}
    } else {

	// Yes, so search the < branch with a child thread.
	long ltCount = 0;
	if (ltChild != NULL) {
#pragma omp task shared(ltCount) firstprivate(dim, maximumSubmitDepth, depth)
	    ltCount = ltChild->verifyKdTree(dim, maximumSubmitDepth, depth + 1);
	}

	// And simultaneously search the > branch with another child thread.
	long gtCount = 0;
	if (gtChild != NULL) {
#pragma omp task shared(gtCount) firstprivate(dim, maximumSubmitDepth, depth)
	    gtCount = gtChild->verifyKdTree(dim, maximumSubmitDepth, depth + 1);
	}

	// Wait for both child threads to finish execution then add their results to the total. 
#pragma omp taskwait
	count += ltCount + gtCount;
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
     * numThreads - the number of threads
     * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
     *
     * returns: a KdNode pointer to the root of the k-d tree
     */
public:
    static KdNode *createKdTree(vector<long*>& coordinates, const long numDimensions,
				const long numThreads, const long maximumSubmitDepth)
    {
	struct timespec startTime, endTime;

	// Initialize the reference arrays using one thread per dimension if possible.
	// Create a temporary array for use in sorting the references and building the k-d tree.
#ifdef MACH
	startTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &startTime);
#endif
	vector< vector<long*> > references(numDimensions, vector<long*>( coordinates.size() ) );
	vector<long*> temporary( coordinates.size() );
	if (numThreads > 1) {
#pragma omp parallel shared(coordinates, references)
	    {
#pragma omp single
		for (long i = 0; i < numDimensions; i++) {
#pragma omp task shared(coordinates, references) firstprivate(i)
		    initializeReference( coordinates, references[i] );
		}
#pragma omp taskwait
	    }
	} else {
	    for (long i = 0; i < numDimensions; i++) {
		initializeReference( coordinates, references[i] );
	    }
	}
#ifdef MACH
	endTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &endTime);
#endif
	double initTime = (endTime.tv_sec - startTime.tv_sec) +
	    1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));

	// Sort the reference array using multiple threads if possible.
#ifdef MACH
	startTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &startTime);
#endif
	if (numThreads > 1) {
	    // Create a parallel region and specify the shared variables.
#pragma omp parallel shared(references, temporary) 
	    {
		// Execute in single-threaded mode until a '#pragma omp task' directive is encountered.
#pragma omp single
		for (long i = 0; i < numDimensions; i++) {
		    mergeSort(references[i], temporary, 0, references[i].size()-1, i, numDimensions,
			      maximumSubmitDepth, 0);
		}
	    }
	} else {
	    for (long i = 0; i < numDimensions; i++) {
		mergeSort(references[i], temporary, 0, references[i].size()-1, i, numDimensions,
			  maximumSubmitDepth, 0);
	    }
	}
#ifdef MACH
	endTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &endTime);
#endif
	double sortTime = (endTime.tv_sec - startTime.tv_sec) +
	    1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));

	// Remove references to duplicate coordinates via one pass through each reference array
	// using multiple threads if possible.
#ifdef MACH
	startTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &startTime);
#endif
	vector<long> end( references.size() );
	if (numThreads > 1) {
#pragma omp parallel shared(references, end)
	    {
#pragma omp single
		for (long i = 0; i < numDimensions; i++) {
#pragma omp task shared(references, end) firstprivate(i, numDimensions)
		    end[i] = removeDuplicates(references[i], i, numDimensions);
		}
#pragma omp taskwait
	    }
	} else {
	    for (long i = 0; i < numDimensions; i++) {
		end[i] = removeDuplicates(references[i], i, numDimensions);
	    }
	}
#ifdef MACH
	endTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &endTime);
#endif
	double removeTime = (endTime.tv_sec - startTime.tv_sec) +
	    1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));

	// Check that the same number of references was removed from each reference array.
	for (long i = 0; i < end.size()-1; i++) {
	    for (long j = i + 1; j < end.size(); j++) {
		if ( end[i] != end[j] ) {
		    cout << "reference removal error" << endl;
		    exit(1);
		}
	    }
	}

#ifdef MACH
	startTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &startTime);
#endif
	KdNode *root = NULL;

	// Build the k-d tree with multiple threads if possible.
	if (numThreads > 1) {
	    // Create a parallel region and specify the shared variables.
#pragma omp parallel shared(root, references, temporary, end)
	    {
		// Execute in single-threaded mode until a '#pragma omp task' directive is encountered.
#pragma omp single
		root = buildKdTree(references, temporary, 0, end[0], numDimensions, maximumSubmitDepth, 0);
	    }
	} else {
	    root = buildKdTree(references, temporary, 0, end[0], numDimensions, maximumSubmitDepth, 0);
	}
#ifdef MACH
	endTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &endTime);
#endif
	double kdTime = (endTime.tv_sec - startTime.tv_sec) +
	    1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));

	// Verify the k-d tree and report the number of kdNodes.
#ifdef MACH
	startTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &startTime);
#endif
	long numberOfNodes;

	// Verify the k-d tree with multiple threads if possible.
	if (numThreads > 1) {
	    // Create a parallel region and specify the shared variables.
#pragma omp parallel shared(numberOfNodes, root)
	    {
		// Execute in single-threaded mode until a '#pragma omp task' directive is encountered.
#pragma omp single
		numberOfNodes = root->verifyKdTree(numDimensions, maximumSubmitDepth, 0);
	    }
	} else {
	    numberOfNodes = root->verifyKdTree(numDimensions, maximumSubmitDepth, 0);
	}
#ifdef MACH
	endTime = mach_gettime();
#else
	clock_gettime(CLOCK_REALTIME, &endTime);
#endif
	double verifyTime = (endTime.tv_sec - startTime.tv_sec) +
	    1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));
	cout << "Number of nodes = " << numberOfNodes << endl << endl;

	cout << "totalTime = " << fixed << setprecision(2) << initTime + sortTime + removeTime + kdTime + verifyTime
	     << "  initTime = " << initTime << "  sortTime = " << sortTime << "  removeTime = " << removeTime
	     << "  kdTime = " << kdTime << "  verifyTime = " << verifyTime << endl << endl;

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
     * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
     * depth - the depth in the k-d tree
     *
     * returns: a list that contains the kdNodes that lie within the cutoff distance of the query node
     */
public:
    list<KdNode> searchKdTree(const long* query, const long cut, const long dim,
			      const long maximumSubmitDepth, const long depth) const {

	// The partition cycles as x, y, z, w...
	long axis = depth % dim;

	// If the distance from the query node to the k-d node is within the cutoff distance
	// in all k dimensions, add the k-d node to a list.
	list<KdNode> result;
	bool inside = true;
	for (long i = 0; i < dim; i++) {
	    if (abs(query[i] - tuple[i]) > cut) {
		inside = false;
		break;
	    }
	}
	if (inside) {
	    result.push_back(*this); // The push_back function expects a KdNode for a call by reference.
	}

	// Search the < branch with a child thread at as many levels of the tree as possible.
	// Create the child thread as high in the tree as possible for greater utilization.

	// Is a child thread available to build the < branch?
	if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

	    // No, so search the < branch of the k-d tree with the current thread if the partition
	    // coordinate of the query point minus the cutoff distance is <= the partition coordinate
	    // of the k-d node.  The < branch must be searched when the cutoff distance equals the
	    // partition coordinate because the super key may assign a point to either branch of the
	    // tree if the sorting or partition coordinate, which forms the most significant portion
	    // of the super key, indicates equality.
	    if ( ltChild != NULL && (query[axis] - cut) <= tuple[axis] ) {
		list<KdNode> ltResult =
		    ltChild->searchKdTree(query, cut, dim, maximumSubmitDepth, depth + 1);
		result.splice(result.end(), ltResult); // Can't substitute searchKdTree(...) for ltResult.
	    }

	    // Then search the > branch of the k-d tree with the current thread if the partition
	    // coordinate of the query point plus the cutoff distance is >= the partition coordinate
	    // of the k-d node.  The > branch must be searched when the cutoff distance equals the
	    // partition coordinate because the super key may assign a point to either branch of the
	    // tree if the sorting or partition coordinate, which forms the most significant portion
	    // of the super key, indicates equality.
	    if ( gtChild != NULL && (query[axis] + cut) >= tuple[axis] ) {
		list<KdNode> gtResult =
		    gtChild->searchKdTree(query, cut, dim, maximumSubmitDepth, depth + 1);
		result.splice(result.end(), gtResult); // Can't substitute searchKdTree(...) for gtResult.
	    }

	} else {

	    // Yes, a child thread is available, so search the < branch with a child thread if the
	    // partition coordinate of the query point minus the cutoff distance is <= the partition
	    // coordinate of the k-d node.  The ltChild, query, cut, maximumSubmitDepth, dim and depth
	    // variables are read only, so they may be shared among threads, or firstprivate which copies
	    // them for each thread. The copy operations are not expensive because there are not many threads.
	    list<KdNode> ltResult;
	    if ( ltChild != NULL && (query[axis] - cut) <= tuple[axis] ) {
#pragma omp task shared(ltResult) firstprivate(query, cut, dim, maximumSubmitDepth, depth)
	        ltResult = ltChild->searchKdTree(query, cut, dim, maximumSubmitDepth, depth + 1);	    }

	    // And simultaneously search the < branch with another child thread if the partition coordinate
	    // of the query point plus the cutoff distance is >= the partition coordinate of the k-d node.
	    list<KdNode> gtResult;
	    if ( gtChild != NULL && (query[axis] + cut) >= tuple[axis] ) {
#pragma omp task shared(gtResult) firstprivate(query, cut, dim, maximumSubmitDepth, depth)
		gtResult = gtChild->searchKdTree(query, cut, dim, maximumSubmitDepth, depth + 1);
	    }

	    // Wait for both child threads to finish execution then append their results.
#pragma omp taskwait
	    result.splice(result.end(), ltResult);
	    result.splice(result.end(), gtResult);
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
	if (gtChild != NULL) {
	    gtChild->printKdTree(dim, depth+1);
	}
	for (long i=0; i<depth; i++) cout << "       ";
	printTuple(tuple, dim);
	cout << endl;
	if (ltChild != NULL) {
	    ltChild->printKdTree(dim, depth+1);
	}
    }
};

/*
 * Create a random long in the interval [min, max].  See
 * http://stackoverflow.com/questions/6218399/how-to-generate-a-random-number-between-0-and-1
 *
 * calling parameters:
 *
 * min - the minimum long value desired
 * max - the maximum long value desired
 *
 * returns: a random long
 */
long randomLongInInterval(const long min, const long max) {
    return min + (long) ((((double) rand()) / ((double) RAND_MAX)) * (max - min));
}

/* Create a simple k-d tree and print its topology for inspection. */
int main(int argc, char **argv)
{
    struct timespec startTime, endTime;

    // Set the defaults then parse the input arguments.
    long numPoints = 262144;
    long extraPoints = 100;
    long numDimensions = 3;
    long numThreads = 5;
    long searchDistance = 2000000000;
    long maximumNumberOfNodesToPrint = 5;

    for (int i = 1; i < argc; i++) {
	if ( 0 == strcmp(argv[i], "-n") || 0 == strcmp(argv[i], "--numPoints") ) {
	    numPoints = atol(argv[++i]);
	    continue;
	}
	if ( 0 == strcmp(argv[i], "-x") || 0 == strcmp(argv[i], "--extraPoints") ) {
	    extraPoints = atol(argv[++i]);
	    continue;
	}
	if ( 0 == strcmp(argv[i], "-d") || 0 == strcmp(argv[i], "--numDimensions") ) {
	    numDimensions = atol(argv[++i]);
	    continue;
	}
	if ( 0 == strcmp(argv[i], "-t") || 0 == strcmp(argv[i], "--numThreads") ) {
	    numThreads = atol(argv[++i]);
	    continue;
	}
	if ( 0 == strcmp(argv[i], "-s") || 0 == strcmp(argv[i], "--searchDistance") ) {
	    
	    continue;
	}
	if ( 0 == strcmp(argv[i], "-p") || 0 == strcmp(argv[i], "--maximumNodesToPrint") ) {
	    maximumNumberOfNodesToPrint = atol(argv[++i]);
	    continue;
	}
	cout << "illegal command-line argument: " <<  argv[i] << endl;
	exit(1);
    }

    // Declare and initialize the coordinates vector and initialize it with (x,y,z,w) tuples
    // in the half-open interval [0, LONG_MAX] where LONG_MAX is defined in limits.h
    // Create extraPoints-1 duplicate coordinates, where extraPoints <= numPoints,
    // in order to test the removal of duplicate points.
    //
    // Note that the tuples are not vectors in order to avoid copying via assignment statements.
    extraPoints = (extraPoints <= numPoints) ? extraPoints : numPoints;
    vector<long*> coordinates(numPoints + extraPoints - 1);
    for (long i = 0; i < coordinates.size(); i++) {
	coordinates[i] = (long *)malloc(numDimensions*sizeof(long));
    }
    for (long i = 0; i < numPoints; i++) {
	for (long j = 0; j < numDimensions; j++) {
	    coordinates[i][j] = randomLongInInterval(0, LONG_MAX);
	}
    }
    for (long i = 1; i < extraPoints; i++) {
	for (long j = 0; j < numDimensions; j++) {
	    coordinates[numPoints - 1 + i][j] = coordinates[numPoints - 1 - i][j];
	}
    }

    // Calculate the number of child threads to be the number of threads minus 1, then
    // calculate the maximum tree depth at which to launch a child thread.  Truncate
    // this depth such that the total number of threads, including the master thread, is
    // an integer power of 2, hence simplifying the launching of child threads by restricting
    // them to only the < branch of the tree for some number of levels of the tree.
    long n = 0;
    if (numThreads > 0) {
	while (numThreads > 0) {
	    n++;
	    numThreads >>= 1;
	}
	numThreads = 1 << (n - 1);
    } else {
	numThreads = 0;
    }
    long childThreads = numThreads - 1;
    long maximumSubmitDepth = -1;
    if (numThreads < 2) {
	maximumSubmitDepth = -1; // The sentinel value -1 specifies no child threads.
    } else if (numThreads == 2) {
	maximumSubmitDepth = 0;
    } else {
	maximumSubmitDepth = (long) floor( log( (double) childThreads) / log(2.) );
    }
    cout << endl << "Number of child threads = " << childThreads << "  maximum submit depth = "
	 << maximumSubmitDepth << endl << endl;

    // Explicitly disable dynamic teams and specify the total number of threads,
    // allowing for one thread for each recursive call.
    if (numThreads > 1) {
	omp_set_dynamic(0);
	omp_set_num_threads(2 * numThreads);
    }

    // Create the k-d tree.
    KdNode *root = KdNode::createKdTree(coordinates, numDimensions, numThreads, maximumSubmitDepth);

    // Search the k-d tree for the k-d nodes that lie within the cutoff distance of the first tuple.
    long* query = (long *) malloc(numDimensions * sizeof(long));
    for (long i = 0; i < numDimensions; i++) {
	query[i] = coordinates[0][i];
    }
#ifdef MACH
    startTime = mach_gettime();
#else
    clock_gettime(CLOCK_REALTIME, &startTime);
#endif
    list<KdNode> kdList;

    // Search the k-d tree with multiple threads if possible.
    if (numThreads > 1) {
	//Create a parallel region and specify the shared variables.
#pragma omp parallel shared(kdList, root, query, searchDistance, numDimensions, maximumSubmitDepth)
	{
	    // Execute in single-threaded mode until a '#pragma omp task' directive is encountered.
#pragma omp single
	    kdList = root->searchKdTree(query, searchDistance, numDimensions, maximumSubmitDepth, 0);
	}
    } else {
	kdList = root->searchKdTree(query, searchDistance, numDimensions, maximumSubmitDepth, 0);
    }
#ifdef MACH
    endTime = mach_gettime();
#else
    clock_gettime(CLOCK_REALTIME, &endTime);
#endif
    double searchTime = (endTime.tv_sec - startTime.tv_sec) +
	1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));
    
    cout << "searchTime = " << fixed << setprecision(2) << searchTime << " seconds" << endl << endl;

    cout << kdList.size() << " nodes within " << searchDistance << " units of ";
    KdNode::printTuple(query, numDimensions);
    cout << " in all dimensions." << endl << endl;
    if (kdList.size() != 0) {
	list<KdNode>::iterator it;
	cout << "List of the first " << maximumNumberOfNodesToPrint << " k-d nodes within a "
	     << searchDistance << "-unit search distance follows:" << endl << endl;
	for (it = kdList.begin(); it != kdList.end(); it++) {
	    KdNode::printTuple(it->getTuple(), numDimensions);
	    cout << endl;
	    maximumNumberOfNodesToPrint--;
	    if (maximumNumberOfNodesToPrint == 0) {
		break;
	    }
    	}
    	cout << endl;
    }	
    return 0;
}
