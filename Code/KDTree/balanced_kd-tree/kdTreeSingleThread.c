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

/* @(#)kdTreeSingleThread.c	1.83 05/07/15 */

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

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

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
    if ( (node = (kdNode_t *) malloc(sizeof(kdNode_t))) == NULL ) {
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
    if ( (listPtr = (listElem_t *) malloc(sizeof(listElem_t))) == NULL ) {
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
 */
void mergeSort(long **reference, long **temporary, const long low, const long high,
	       const long p, const long dim)
{
    long i, j, k;

    if (high > low) {

	// Avoid overflow when calculating the median.
	const long mid = low + ( (high - low) >> 1 );

	// Recursively subdivide the lower half of the array.
	mergeSort(reference, temporary, low, mid, p, dim);

	// Recursively subdivide the upper half of the array.
	mergeSort(reference, temporary, mid+1, high, p, dim);

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
 * references - arrays of references to coordinate tuples
 * temp - a temporary array for copying one of the reference arrays
 * start - start element of the reference arrays
 * end - end element of the reference arrays
 * dim - the number of dimensions to sort
 * depth - the depth in the tree
 *
 * returns: a kdNode pointer to the root of the k-d tree
 */
kdNode_t *buildKdTree(long ***references, long **temp, const long start, const long end,
		      const long dim, const long depth)
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
	    temp[i] = references[0][i];
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
	    references[dim-1][i] = temp[i];
    	}

    	// Recursively build the < branch of the tree.
    	node->ltChild = buildKdTree(references, temp, start, lower, dim, depth+1);

    	// Recursively build the > branch of the tree.
    	node->gtChild = buildKdTree(references, temp, median+1, upper, dim, depth+1);

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
 * depth - the depth in the k-d tree 
 *
 * returns: a count of the number of kdNodes in the k-d tree
 */
long verifyKdTree(const kdNode_t *node, const long dim, const long depth)
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
    	count += verifyKdTree(node->ltChild, dim, depth + 1);
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
    	count += verifyKdTree(node->gtChild, dim, depth + 1);
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
 *
 * returns: a kdNode_t pointer to the root of the k-d tree
 */
static kdNode_t *createKdTree(long **coordinates, const long n, const long numDimensions)
{
    // Initialize and sort the reference arrays.
    long ***references = (long ***) malloc(numDimensions * sizeof(long **));
    long **temp = (long ** ) malloc(n * sizeof(long));
    for (long i=0; i<numDimensions; i++) {
    	references[i] = (long **) malloc(n * sizeof(long *));
	initializeReference(coordinates, references[i], n);
	mergeSort(references[i], temp, 0, n-1, i, numDimensions);
    }

    // Remove references to duplicate coordinates via one pass through each reference array.
    long *end = (long *) malloc(numDimensions * sizeof(long));
    for (long i = 0; i < numDimensions; i++) {
    	end[i] = removeDuplicates(references[i], n, i, numDimensions);
    }

    // Check that the same number of references was removed from each reference array.
    for (long i = 0; i < numDimensions - 1; i++) {
    	for (long j = i + 1; j < numDimensions; j++) {
	    if (end[i] != end[j]) {
		printf("Reference removal error\n");
		exit(1);
	    }
    	}
    }

    // Build the k-d tree.
    kdNode_t *root = buildKdTree(references, temp, 0, end[0], numDimensions, 0);

    // Free the reference and temporary arrays.
    for (long i = 0; i < numDimensions; i++) {
	free(references[i]);
    }
    free(references);
    free(temp);

    // Verify the k-d tree and report the number of kdNodes.
    long numberOfNodes = verifyKdTree(root, numDimensions, 0);
    printf("\nNumber of nodes = %ld\n", numberOfNodes);

    // Return the root of the k-d tree.
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
 * node - pointer to the kdNode being visited
 * query - the query point
 * cut - the cutoff distance
 * dim - the number of dimensions
 * depth - the depth in the k-d tree
 *
 * returns: a list that contains the kdNodes that lie within the cutoff distance of the query node
 */
listElem_t *searchKdTree(const kdNode_t *node, const long* query, const long cut,
			 const long dim, const long depth) {

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

    // Search the < branch of the k-d tree if the partition coordinate of the query point minus
    // the cutoff distance is <= the partition coordinate of the k-d node.  The < branch must be
    // searched when the cutoff distance equals the partition coordinate because the super key
    // may assign a point to either branch of the tree if the sorting or partition coordinate,
    // which forms the most significant portion of the super key, shows equality.
    if ( node->ltChild != NULL && (query[axis] - cut) <= node->tuple[axis] ) {
    	result = appendToList( result, searchKdTree(node->ltChild, query, cut, dim, depth + 1) );
    }

    // Search the > branch of the k-d tree if the partition coordinate of the query point plus
    // the cutoff distance is >= the partition coordinate of the k-d node.  The < branch must be
    // searched when the cutoff distance equals the partition coordinate because the super key
    // may assign a point to either branch of the tree if the sorting or partition coordinate,
    // which forms the most significant portion of the super key, shows equality.
    if ( node->gtChild != NULL && (query[axis] + cut) >= node->tuple[axis] ) {
    	result = appendToList( result, searchKdTree(node->gtChild, query, cut, dim, depth + 1) );
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
    for (long i=1; i<dim-1; i++) printf("%ld,", tuple[i]);
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

#define DIMENSIONS (3)
#define NUM_TUPLES (25)
#define SEARCH_DISTANCE (2)

/* Create a simple k-d tree and print its topology for inspection. */
int main(int argc, char **argv)
{
    // Declare the coordinates array that contains (x,y,z) coordinates.
    long coordinates[NUM_TUPLES][DIMENSIONS] = {
	{2,3,3}, {5,4,2}, {9,6,7}, {4,7,9}, {8,1,5},
	{7,2,6}, {9,4,1}, {8,4,2}, {9,7,8}, {6,3,1},
	{3,4,5}, {1,6,8}, {9,5,3}, {2,1,3}, {8,7,6},
	{5,4,2}, {6,3,1}, {8,7,6}, {9,6,7}, {2,1,3},
	{7,2,6}, {4,7,9}, {1,6,8}, {3,4,5}, {9,4,1} };

    // Create the k-d tree.  Note pointer manipulation to pass a 2D array.
    long **coordinatePtr = (long **) malloc(NUM_TUPLES * sizeof(long*));
    for (long i = 0; i < NUM_TUPLES; i++) {
	coordinatePtr[i] = &(coordinates[i][0]);
    }
    kdNode_t *root = createKdTree(coordinatePtr, NUM_TUPLES, DIMENSIONS);

    // Print the k-d tree "sideways" with the root at the left.
    printf("\n");
    printKdTree(root, DIMENSIONS, 0);

    // Search the k-d tree for the k-d nodes that lie within the cutoff distance.
    long query[3] = {4, 3, 1};
    listElem_t *kdList = searchKdTree(root, query, SEARCH_DISTANCE, DIMENSIONS, 0);
    listElem_t *kdWalk = kdList;
    long kdCount = 0;
    while (kdWalk != NULL) {
    	kdCount++;
    	kdWalk = kdWalk->next;
    }
    printf("\n%ld nodes within %ld units of ", kdCount, SEARCH_DISTANCE);
    printTuple(query, DIMENSIONS);
    printf(" in all dimensions.\n\n");
    if (kdCount != 0) {
    	printf("List of k-d nodes within %ld-unit search distance follows:\n\n", SEARCH_DISTANCE);
    	kdWalk = kdList;
    	while (kdWalk != NULL) {
	    printTuple(kdWalk->node->tuple, DIMENSIONS);
	    printf(" ");
	    kdWalk = kdWalk->next;
    	}
    	printf("\n\n");
    }	
    return 0;
}
