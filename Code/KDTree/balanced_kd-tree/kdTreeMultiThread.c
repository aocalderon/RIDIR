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

/* @(#)kdTreeMultiThread.c	1.41 05/07/15 */

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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>
#include <math.h>
#include <omp.h>
#include <limits.h>
#include <string.h>

/* One node of a k-d tree */
typedef struct kdNode
{
    const long *tuple;
    const struct kdNode *ltChild, *gtChild;
} kdNode_t;

/* One element of a list of kdNodes */
typedef struct listElem
{
    const kdNode_t *node;
    struct listElem *last, *next;
} listElem_t;

/*
 * This function allocates and initializes a kdNode structure.
 *
 * calling parameters:
 *
 * tuple - a tuple to store in the kdNode
 *
 * returns: a kdNode
 */
kdNode_t *newKdNode(const long *tuple)
{
    kdNode_t *node;
    if ( (node = (kdNode_t *)malloc(sizeof(kdNode_t))) == NULL ) {
    	printf("error allocating kdNode!\n");
    	exit(1);
    }
    node->tuple = tuple;
    node->ltChild = node->gtChild = NULL;
    return node;
}

/*
 * This function allocates and initializes a listElem structure.
 *
 * calling parameters:
 *
 * node - a kdNode
 *
 * returns: a listElem
 */
listElem_t *newListElem(const kdNode_t *node)
{
    listElem_t *listPtr;
    if ( (listPtr = (listElem_t *)malloc(sizeof(listElem_t))) == NULL ) {
    	printf("error allocating listPtr!\n");
    	exit(1);
    }
    listPtr->node = node;
    listPtr->last = listPtr;
    listPtr->next = NULL;
    return listPtr;
}

/*
 * Initialize a reference array by creating references into the coordinates array.
 *
 * calling parameters:
 *
 * coordinates - the array of (x, y, z, w...) coordinates
 * reference - one reference array
 * n - the number of points
 */
void initializeReference(long **coordinates, long **reference, const long n)
{
    for (long j = 0; j < n; j++) {
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
long superKeyCompare(const long* a, const long* b, const long p, const long dim)
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
 * reference - the array to sort
 * temporary - a temporary array into which to copy intermediate results
 *             this array must be as large as the reference array
 * low - the start index of the region of the reference array to sort
 * high - the high index of the region of the reference array to sort
 * p - the sorting partition (x, y, z, w...)
 * dim - the number of dimensions
 * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
 * depth - the depth of subdivision
 */
void mergeSort(long **reference, long **temporary, const long low, const long high, const long p,
	       const long dim, const long maximumSubmitDepth, const long depth)
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
		(superKeyCompare(temporary[i], temporary [j], p, dim) < 0) ? temporary[i++] : temporary[j--];
	}
    }
}

/*
 * Check the validity of the merge sort and remove duplicates from a reference array.
 *
 * calling parameters:
 *
 * reference - one reference array
 * n - the number of points
 * i - the leading dimension for the super key
 * dim - the number of dimensions
 *
 * returns: the end index of the reference array following removal of duplicate elements
 */
long removeDuplicates(long **reference, const long n, const long i, const long dim)
{
    long end = 0;
    for (long j = 1; j < n; j++) {
	long compare = superKeyCompare(reference[j], reference[j-1], i, dim);
	if (compare < 0) {
	    printf("merge sort failure: superKeyCompare(ref[%ld], ref[%ld], (%ld) = %ld\n",
		   j, j-1, i, compare);
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
 * ref - arrays of references to coordinate tuples
 * temporary - a temporary array for copying one of the reference arrays
 * start - start element of the reference arrays
 * end - end element of the reference arrays
 * dim - the number of dimensions to sort
 * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
 * depth - the depth in the tree
 *
 * returns: a kdNode_t pointer to the root of the k-d tree.
 */
kdNode_t *buildKdTree(long ***references, long **temporary, const long start, const long end,
		      const long dim, const long maximumSubmitDepth, const long depth)
{
    kdNode_t *node;

    // The axis permutes as x, y, z, w... and addresses the referenced data.
    long axis = depth % dim;

    if (end == start) {

	// Only one reference was passed to this function, so add it to the tree.
	node = newKdNode(references[0][end]);

    } else if (end == start + 1) {

    	// Two references were passed to this function in sorted order, so store the start
    	// element at this level of the tree and store the end element as the > child.
	node = newKdNode(references[0][start]);
	node->gtChild = newKdNode(references[0][end]);

    } else if (end == start + 2) {

	// Three references were passed to this function in sorted order, so
	// store the median element at this level of the tree, store the start
	// element as the < child and store the end element as the > child.
	node = newKdNode(references[0][start + 1]);
	node->ltChild = newKdNode(references[0][start]);
	node->gtChild = newKdNode(references[0][end]);

    } else if (end > start + 2) {

    	// More than three references were passed to this function, so
    	// the median element of references[0] is chosen as the tuple about
    	// which the other reference arrays will be partitioned.  Avoid
    	// overflow when computing the median.
    	const long median = start + ( (end - start) >> 1 );

    	// Store the median element of references[0] in a new kdNode.
    	node = newKdNode(references[0][median]);

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
		printf("incorrect range for lower at depth = %ld : start = %ld  lower = %ld  median = %ld\n",
		       depth, start, lower, median);
		exit(1);
	    }

	    if (upper <= median || upper > end) {
		printf("incorrect range for upper at depth = %ld : median = %ld  upper = %ld  end = %ld\n",
		       depth, median, upper, end);
		exit(1);
	    }

	    if (i > 1 && lower != lowerSave) {
		printf("lower = %ld  !=  lowerSave = %ld\n", lower, lowerSave);
		exit(1);
	    }

	    if (i > 1 && upper != upperSave) {
		printf("upper = %ld  !=  upperSave = %ld\n", upper, upperSave);
		exit(1);
	    }

	    lowerSave = lower;
	    upperSave = upper;
    	}

        // Copy the temporary array to references[dim-1] to finish permutation.
        for (long i = start; i <= end; i++) {
	    references[dim-1][i] = temporary[i];
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
    	printf("error has occurred at depth = %ld : end = %ld  <  start = %ld\n",
	       depth, end, start);
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
 * node - pointer to the kdNode being visited
 * dim - the number of dimensions
 * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
 * depth - the depth in the k-d tree 
 *
 * returns: a count of the number of kdNodes in the k-d tree
 */
long verifyKdTree(const kdNode_t *node, const long dim, const long maximumSubmitDepth, const long depth)
{
    long count = 1 ;
    if (node->tuple == NULL) {
    	printf("point is null\n");
    	exit(1);
    }

    // The partition cycles as x, y, z, w...
    long axis = depth % dim;

    if (node->ltChild != NULL) {
    	if (node->ltChild->tuple[axis] > node->tuple[axis]) {
	    printf("child is > node!\n");
	    exit(1);
	}
	if (superKeyCompare(node->ltChild->tuple, node->tuple, axis, dim) >= 0) {
	    printf("child is >= node!\n");
	    exit(1);
	}
    }
    if (node->gtChild != NULL) {
    	if (node->gtChild->tuple[axis] < node->tuple[axis]) {
	    printf("child is < node!\n");
	    exit(1);
	}
	if (superKeyCompare(node->gtChild->tuple, node->tuple, axis, dim) <= 0) {
	    printf("child is <= node!\n");
	    exit(1);
	}
    }

    // Search the < branch with a child thread at as many levels of the tree as possible.
    // Create the child thread as high in the tree as possible for greater utilization.

    // Is a child thread available to build the < branch?
    if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

	// No, so search the < branch with the current thread.
	if (node->ltChild != NULL) {
	    count += verifyKdTree(node->ltChild, dim, maximumSubmitDepth, depth + 1);
	}

	// Then search the > branch with the current thread.
	if (node->gtChild != NULL) {
	    count += verifyKdTree(node->gtChild, dim, maximumSubmitDepth, depth + 1);
	}
    } else {

	// Yes, so search the < branch with a child thread.
	long ltCount = 0;
	if (node->ltChild != NULL) {
#pragma omp task shared(ltCount) firstprivate(node, dim, maximumSubmitDepth, depth)
	    ltCount = verifyKdTree(node->ltChild, dim, maximumSubmitDepth, depth + 1);
	}

	// And simultaneously search the > branch with another child thread.
	long gtCount = 0;
#pragma omp task shared(gtCount) firstprivate(node, dim, maximumSubmitDepth, depth)
	if (node->gtChild != NULL) {
	    gtCount = verifyKdTree(node->gtChild, dim, maximumSubmitDepth, depth + 1);
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
 * coordinates - array of (x, y, z, w...) coordinates
 * n - the number of points
 * numDimensions - the number of dimensions
 * numThreads - the number of threads that are available to execute tasks
 * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
 *
 * returns: a kdNode_t pointer to the root of the k-d tree
 */
static kdNode_t *createKdTree(long **coordinates, const long n, const long numDimensions,
			      const long numThreads, const long maximumSubmitDepth)
{
    struct timespec startTime, endTime;

    // Initialize the reference arrays using one thread per dimension if possible.
    // Create a temporary array for use in sorting the references and building the k-d tree.
    clock_gettime(CLOCK_REALTIME, &startTime);
    long **temporary = (long **)malloc(n*sizeof(long *));
    long ***references = (long ***)malloc(numDimensions*sizeof(long **));
    for (long i = 0; i < numDimensions; i++) {
	references[i] = (long **)malloc(n*sizeof(long *));
    }
    if (numThreads > 1) {
#pragma omp parallel shared(coordinates, references)
	{
#pragma omp single
	    for (long i = 0; i < numDimensions; i++) {
#pragma omp task shared(coordinates, references) firstprivate(n, i)
		initializeReference(coordinates, references[i], n);
	    }
#pragma omp taskwait
	}
    } else {
	for (long i = 0; i < numDimensions; i++) {
	    initializeReference(coordinates, references[i], n);
	}
    }
    clock_gettime(CLOCK_REALTIME, &endTime);
    double initTime = (endTime.tv_sec - startTime.tv_sec) +
	1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));

    // Sort the reference array using multiple threads if possible.
    clock_gettime(CLOCK_REALTIME, &startTime);
    if (numThreads > 1) {
	// Create a parallel region and specify the shared variables.
#pragma omp parallel shared(references, temporary) 
	{
	    // Execute in single-threaded mode until a '#pragma omp task' directive is encountered.
#pragma omp single
	    for (long i = 0; i < numDimensions; i++) {
		mergeSort(references[i], temporary, 0, n-1, i, numDimensions, maximumSubmitDepth, 0);
	    }
	}
    } else {
	for (long i = 0; i < numDimensions; i++) {
	    mergeSort(references[i], temporary, 0, n-1, i,numDimensions, maximumSubmitDepth, 0);
	}
    }
    clock_gettime(CLOCK_REALTIME, &endTime);
    double sortTime = (endTime.tv_sec - startTime.tv_sec) +
	1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));

    // Remove references to duplicate coordinates via one pass through each reference array
    // using multiple threads if possible.
    clock_gettime(CLOCK_REALTIME, &startTime);
    long end[numDimensions];
    if (numThreads > 1) {
#pragma omp parallel shared(references, end)
	{
#pragma omp single
	    for (long i = 0; i < numDimensions; i++) {
#pragma omp task shared(references, end) firstprivate(n, i, numDimensions)
		end[i] = removeDuplicates(references[i], n, i, numDimensions);
	    }
#pragma omp taskwait
	}
    } else {
	for (long i = 0; i < numDimensions; i++) {
	    end[i] = removeDuplicates(references[i], n, i, numDimensions);
	}
    }
    clock_gettime(CLOCK_REALTIME, &endTime);
    double removeTime = (endTime.tv_sec - startTime.tv_sec) +
	1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));

    // Check that the same number of references was removed from each reference array.
    for (long i = 0; i < numDimensions - 1; i++) {
	for (long j = i + 1; j < numDimensions; j++) {
	    if (end[i] != end[j]) {
		printf("Reference removal error\n");
		exit(1);
	    }
	}
    }

    clock_gettime(CLOCK_REALTIME, &startTime);
    kdNode_t *root = NULL;

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
    clock_gettime(CLOCK_REALTIME, &endTime);
    double kdTime = (endTime.tv_sec - startTime.tv_sec) +
	1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));

    // Free the reference and temporary arrays.
    for (long i = 0; i < numDimensions; i++) {
	free(references[i]);
    }
    free(references);
    free(temporary);

    // Verify the k-d tree and report the number of kdNodes.
    clock_gettime(CLOCK_REALTIME, &startTime);
    long numberOfNodes;

    // Verify the k-d tree with multiple threads if possible.
    if (numThreads > 1) {
	// Create a parallel region and specify the shared variables.
#pragma omp parallel shared(numberOfNodes, root)
	{
	    // Execute in single-threaded mode until a '#pragma omp task' directive is encountered.
#pragma omp single
	    numberOfNodes = verifyKdTree(root, numDimensions, maximumSubmitDepth, 0);
	}
    } else {
	numberOfNodes = verifyKdTree(root, numDimensions, maximumSubmitDepth, 0);
    }
    clock_gettime(CLOCK_REALTIME, &endTime);
    double verifyTime = (endTime.tv_sec - startTime.tv_sec) +
	1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));
    printf("Number of nodes = %ld\n\n", numberOfNodes);

    // Report the execution times.
    printf("totalTime = %.2f  initTime = %.2f  sortTime = %.2f  removeTime = %.2f  kdTime = %.2f  verifyTime = %.2f\n\n",
	   initTime + sortTime + removeTime + kdTime + verifyTime, initTime, sortTime, removeTime, kdTime, verifyTime);

    // Return the pointer to the root of the k-d tree.
    return root;
}

/*
 * Append one list to another list.  The 'last' pointer references the last
 * element of the list, but is correct only for the first element of the list.
 * It allows an append operation without first walking to the end of a list.
 *
 * calling parameters:
 *
 * listA - a list of listElem_t
 * listB - a list of listElem_t
 *
 * returns: the first element of (listA + listB)
 */
listElem_t *appendToList(listElem_t *listA, listElem_t *listB)
{
    if (listA == NULL) {
	return listB;
    }
    if (listB == NULL) {
	return listA;
    }
    listA->last->next = listB;
    listA->last = listB->last;
    return listA;
}

/*
 * Search the k-d tree and find the KdNodes that lie within a cutoff distance
 * from a query node in all k dimensions.
 *
 * calling parameters:
 *
 * node - the node
 * query - the query point
 * cut - the cutoff distance
 * dim - the number of dimensions
 * maximumSubmitDepth - the maximum tree depth at which a child task may be launched
 * depth - the depth in the k-d tree
 *
 * returns: a list that contains the kdNodes that lie within the cutoff distance of the query node
 */
listElem_t *searchKdTree(const kdNode_t *node, const long* query, const long cut,
			 const long dim, const long maximumSubmitDepth, const long depth) {

    // The partition cycles as x, y, z, etc.
    long axis = depth % dim;

    // If the distance from the query node to the k-d node is within the cutoff distance
    // in all k dimensions, add the k-d node to a list.
    listElem_t *result = NULL;
    bool inside = true;
    for (long i = 0; i < dim; i++) {
	if (abs(query[i] - node->tuple[i]) > cut) {
	    inside = false;
	    break;
	}
    }
    if (inside) {
	result = newListElem(node);
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
	if ( node->ltChild != NULL && (query[axis] - cut) <= node->tuple[axis] ) {
	    result = appendToList( result, searchKdTree(node->ltChild, query, cut, dim,
							maximumSubmitDepth, depth + 1) );
	}

	// Then search the > branch of the k-d tree with the current thread if the partition
	// coordinate of the query point plus the cutoff distance is >= the partition coordinate
	// of the k-d node.  The > branch must be searched when the cutoff distance equals the
	// partition coordinate because the super key may assign a point to either branch of the
	// tree if the sorting or partition coordinate, which forms the most significant portion
	// of the super key, indicates equality.
	if ( node->gtChild != NULL && (query[axis] + cut) >= node->tuple[axis] ) {
	    result = appendToList( result, searchKdTree(node->gtChild, query, cut, dim,
							maximumSubmitDepth, depth + 1) );
	}

    } else {

	// Yes, a child thread is available, so search the < branch with a child thread if the
	// partition coordinate of the query point minus the cutoff distance is <= the partition
	// coordinate of the k-d node.  The ltChild, query, cut, maximumSubmitDepth, dim and depth
	// variables are read only, so they may be shared among threads, or firstprivate which copies
	// them for each thread. The copy operations are not expensive because there are not many threads.
	listElem_t *ltResult = NULL;
	if ( node->ltChild != NULL && (query[axis] - cut) <= node->tuple[axis] ) {
#pragma omp task shared(ltResult) firstprivate(node, query, cut, dim, maximumSubmitDepth, depth)
	    ltResult = searchKdTree(node->ltChild, query, cut, dim, maximumSubmitDepth, depth + 1);
	}

	// And simultaneously search the < branch with another child thread if the/ partition coordinate
	// of the query point plus the cutoff distance is >= the partition coordinate of the k-d node.
	listElem_t *gtResult = NULL;
	if ( node->gtChild != NULL && (query[axis] + cut) >= node->tuple[axis] ) {
	    gtResult = appendToList( result, searchKdTree(node->gtChild, query, cut, dim,
							  maximumSubmitDepth, depth + 1) );
	}

	// Wait for both child threads to finish execution then append their results.
#pragma omp taskwait
	result = appendToList(result, ltResult);
	result = appendToList(result, gtResult);
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
void printTuple(const long* tuple, const long dim)
{
    printf("(%ld,", tuple[0]);
    for (long i=1; i<dim-1; i++) printf("%ld, ", tuple[i]);
    printf("%ld)", tuple[dim-1]);
}

/*
 * Print the k-d tree "sideways" with the root at the ltChild.
 *
 * calling parameters:
 *
 * node - pointer to the kdNode being visited
 * dim - the number of dimensions
 * depth - the depth in the k-d tree 
 */
void printKdTree(const kdNode_t *node, const long dim, const long depth)
{
    if (node) {
	printKdTree(node->gtChild, dim, depth+1);
	for (long i=0; i<depth; i++) printf("       ");
	printTuple(node->tuple, dim);
	printf("\n");
	printKdTree(node->ltChild, dim, depth+1);
    }
}

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

/*
 * Create a simple k-d tree and print its topology for inspection.
 */
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
	    searchDistance = atol(argv[++i]);
	    continue;
	}
	if ( 0 == strcmp(argv[i], "-p") || 0 == strcmp(argv[i], "--maximumNodesToPrint") ) {
	    maximumNumberOfNodesToPrint = atol(argv[++i]);
	    continue;
	}
	printf("illegal command-line argument: %s\n", argv[i]);
	exit(1);
    }

    // Declare and initialize the coordinates array and initialize it with (x,y,z,w) tuples
    // in the half-open interval [0, LONG_MAX] where LONG_MAX is defined in limits.h
    // Create extraPoints-1 duplicate coordinates, where extraPoints <= numPoints,
    // in order to test the removal of duplicate points.
    extraPoints = (extraPoints <= numPoints) ? extraPoints : numPoints;
    long **coordinates = (long **)malloc( (numPoints + extraPoints - 1)*sizeof(long *));
    for (long i = 0; i < numPoints + extraPoints - 1; i++) {
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
    printf("\nNumber of child threads = %ld  maximum submit depth = %ld\n\n", childThreads, maximumSubmitDepth);

    // Explicitly disable dynamic teams and specify the total number of threads,
    // allowing for one thread for each recursive call.
    if (numThreads > 1) {
	omp_set_dynamic(0);
	omp_set_num_threads(2 * numThreads);
    }

    // Create the k-d tree.
    kdNode_t *root = createKdTree(coordinates, numPoints + extraPoints - 1, numDimensions,
				  numThreads, maximumSubmitDepth);

    // Search the k-d tree for the k-d nodes that lie within the cutoff distance of the first tuple.
    long query[numDimensions];
    for (long i = 0; i < numDimensions; i++) {
	query[i] = coordinates[0][i];
    }
    clock_gettime(CLOCK_REALTIME, &startTime);
    listElem_t *kdList = NULL;

    // Search the k-d tree with multiple threads if possible.
    if (numThreads > 1) {
	//Create a parallel region and specify the shared variables.
#pragma omp parallel shared(kdList, root, query, searchDistance, numDimensions, maximumSubmitDepth)
	{
	    // Execute in single-threaded mode until a '#pragma omp task' directive is encountered.
#pragma omp single
	    kdList = searchKdTree(root, query, searchDistance, numDimensions, maximumSubmitDepth, 0);
	}
    } else {
	kdList = searchKdTree(root, query, searchDistance, numDimensions, maximumSubmitDepth, 0);
    }
    clock_gettime(CLOCK_REALTIME, &endTime);
    double searchTime = (endTime.tv_sec - startTime.tv_sec) +
	1.0e-9 * ((double)(endTime.tv_nsec - startTime.tv_nsec));
    
    printf("searchTime = %.2f seconds\n", searchTime);

    // Count the number of k-d nodes that the search found.
    listElem_t *kdWalk = kdList;
    long kdCount = 0;
    while (kdWalk != NULL) {
	kdCount++;
	kdWalk = kdWalk->next;
    }
    printf("\n%ld nodes within %ld units of ", kdCount, searchDistance);
    printTuple(query, numDimensions);
    printf(" in all dimensions.\n\n");
    if (kdCount != 0) {
	printf("List of the first %ld k-d nodes within a %ld-unit search distance follows:\n\n",
	       maximumNumberOfNodesToPrint, searchDistance);
	kdWalk = kdList;
	while (kdWalk != NULL && maximumNumberOfNodesToPrint != 0) {
	    printTuple(kdWalk->node->tuple, numDimensions);
	    printf("\n");
	    kdWalk = kdWalk->next;
	    maximumNumberOfNodesToPrint--;
	}
	printf("\n");
    }	
    return 0;
}
