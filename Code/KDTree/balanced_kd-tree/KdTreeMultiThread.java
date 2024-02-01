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

/* @(#)KdTreeMultiThread.java	1.51 04/25/15 */

import java.lang.System;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * <p>
 * The k-d tree was described by Jon Bentley in "Multidimensional Binary Search Trees Used
 * for Associative Searching", CACM 18(9): 509-517, 1975.  For k dimensions and n elements
 * of data, a balanced k-d tree is built in O(kn log n) + O((k+1)n log n) time by first
 * sorting the data in each of k dimensions, then building the k-d tree in a manner that
 * preserves the order of the k sorts while recursively partitioning the data at each level
 * of the k-d tree.  No further sorting is necessary.  Moreover, it is possible to replace
 * the O((k+1)n log n) term with a O((k-1)n log n) term but this approach sacrifices the
 * generality of building the k-d tree for points of any number of dimensions.
 * </p>
 *
 * @author Russell A. Brown
 */
class KdTreeMultiThread {
	
	/**
	 * <p>
	 * The {@code KdNode} class stores a point of any number of dimensions
	 * as well as references to the "less than" and "greater than" sub-trees.
	 * </p>
	 */
	static class KdNode {
		
		private final int[] point;
		private KdNode ltChild, gtChild;
		/**
		 * <p>
		 * The {@code KdNode} constructor returns one {@code KdNode}.
		 * </p>
		 * 
		 * @param point - the multi-dimensional point to store in the {@code KdNode}
		 */
		public KdNode(final int[] point) {
			this.point = point;
			ltChild = gtChild = null;
		}
		
		/**
		 * <p>
		 * The {@code initializeReference} method initializes one reference array.
		 * </p>
		 * 
		 * @param coordinates - an array of (x,y,z,w...) coordinates
		 * @param reference - an array of references to the (x,y,z,w...) coordinates
		 * @param i - the index of the most significant coordinate in the super key
		 */
		private static void initializeReference(final int[][] coordinates,
				final int[][] reference, final int i) {
            for (int j = 0; j < reference.length; j++) {
                reference[j] = coordinates[j];
            }
		}

		/**
		 * <p>
         * The {@code initializeReferenceWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the 
         * {@link KdNode#initializeReference initializeReference} method.
         * </p>
         * 
         * @param coordinates - an array of (x,y,z,w...) coordinates
         * @param reference - an array of references to the (x,y,z,w...) coordinates
         * @param i - the index of the most significant coordinate in the super key
         */
        private static Callable<Void> initializeReferenceWithThread(final int[][] coordinates,
        		final int[][] reference, final int i) {
            
            return new Callable<Void>() {
                @Override
                public Void call() {
                    initializeReference(coordinates, reference, i);
                    return null;
                }
            };
        }
        
		/**
		 * <p>
		 * The {@code superKeyCompare} method compares two int[] in as few coordinates as possible
		 * and uses the sorting or partition coordinate as the most significant coordinate.
		 * </p>
		 * 
		 * @param a - a int[]
		 * @param b - a int[]
		 * @param p - the most significant dimension
		 * @returns an int that represents the result of comparing two super keys
		 */
		private static int superKeyCompare(final int[] a, final int[] b, final int p) {
            int diff = 0;
            for (int i = 0; i < a.length; i++) {
                // A fast alternative to the modulus operator for (i + p) < 2 * a.length.
                final int r = (i + p < a.length) ? i + p : i + p - a.length;
                diff = a[r] - b[r];
                if (diff != 0) {
                    break;
                }
            }
            return diff;
		}

		/**
		 * <p>
		 * The {@code mergeSort} function recursively subdivides the array to be sorted then merges the elements.
		 * Adapted from Robert Sedgewick's "Algorithms in C++" p. 166. Addison-Wesley, Reading, MA, 1992.
		 * </p>
		 * 
		 * @param reference - an array of references to the (x,y,z,w...) coordinates
		 * @param temporary - a scratch array into which to copy references;
		 * this array must be as large as the reference array
		 * @param low - the start index of the region of the reference array
		 * @param high - the high index of the region of the reference array
		 * @param p - the sorting partition (x, y, z, w...)
		 * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
		 * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
		 * @param depth - the depth of subdivision
		 */
    	private static void mergeSort(final int[][] reference, final int[][] temporary,
    			final int low, final int high,
    			final int p, final ExecutorService executor, final int maximumSubmitDepth, int depth) {
    		
    		final int INSERTION_SORT_CUTOFF = 15;
    		
			if (high - low > INSERTION_SORT_CUTOFF) {
    			
    			// Avoid overflow when calculating the median address.
    			final int mid = low + ( (high - low) >> 1 );
    			
				// Subdivide the lower half of the tree with a child thread at as many levels of the tree as possible.
				// Create the child threads as high in the subdivision hierarchy as possible for greater utilization.

				// Is a child thread available to subdivide the lower half of the array?
				if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

					// No, so recursively subdivide the lower half of the array with the current thread then
					// copy the result from the reference array to the temporary array in ascending order.
	    			mergeSort(reference, temporary, low, mid, p, executor, maximumSubmitDepth, depth + 1);
	                for (int i = low; i <= mid; i++) {
	                    temporary[i] = reference[i];
	    			}

					// Then recursively subdivide the upper half of the array with the current thread then
	                // copy the result from the reference array to the temporary array in descending order.
	    			mergeSort(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);
	    			for (int j = mid; j < high; j++) {
						temporary[mid + (high - j)] = reference[j + 1]; // Avoid address overflow.
	    			}
	    			 
				} else {

					// Yes, a child thread is available, so recursively subdivide the lower half of the array with a child
					// thread then copy the result from the reference array to the temporary array in ascending order.
					final Future<Void> future = executor.submit( mergeSortWithThread(reference, temporary,
							low, mid, p, executor, maximumSubmitDepth, depth + 1) );

					// And simultaneously, recursively subdivide the upper half of the array with the current thread
	                // then copy the result from the reference array to the temporary array in descending order.
	    			mergeSort(reference, temporary, mid + 1, high, p, executor, maximumSubmitDepth, depth + 1);
	    			for (int j = mid; j < high; j++) {
						temporary[mid + (high - j)] = reference[j + 1]; // Avoid address overflow.
	    			}
	    			 
					// Then get the result of subdividing the lower half of the array with the child thread.
					try {
						future.get();
					} catch (Exception e) {
						throw new RuntimeException( "future exception: " + e.getMessage() );
					}
		        }
    			
				// Merge the results of this level of subdivision.
                for (int i = low, j = high, k = low; k <= high; k++) {
    				reference[k] =
    						(superKeyCompare(temporary[i], temporary[j], p) < 0) ? temporary[i++] : temporary[j--];
    			}
			} else {
				
				// Here is Jon Benley's implementation of insertion sort from "Programming Pearls", pp. 115-116,
				// Addison-Wesley, 1999.
			    for (int i = low + 1; i <= high; i++) {
			        int[] tmp = reference[i];
			        int j;
			        for (j = i; j > low && superKeyCompare(reference[j-1], tmp, p) > 0; j--) {
			            reference[j] = reference[j-1];
			        }
			        reference[j] = tmp;
			    }
    		}
    	}
		
		/**
		 * <p>
		 * The {@code mergeSortWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the 
         * {@link KdNode#mergeSort mergeSort} method.
         * </p>
         * 
		 * @param reference - an array of references to the (x,y,z,w...) coordinates
		 * @param temporary - a scratch array into which to copy references;
		 * this array must be as large as the reference array.
		 * @param low - the start index of the region of the reference array
		 * @param high - the high index of the region of the reference
		 * @param p - the sorting partition (x, y, z, w...)
		 * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
		 * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
		 * @param depth - the depth of subdivision
         */
        private static Callable<Void> mergeSortWithThread(final int[][] reference, final int[][] temporary,
        		final int low, final int high, final int p,
        		final ExecutorService executor, final int maximumSubmitDepth, final int depth) {
            
            return new Callable<Void>() {
                @Override
                public Void call() {
                    mergeSort(reference, temporary, low, high, p, executor, maximumSubmitDepth, depth);
					// Copy the result from the reference array to the temporary array in ascending order.
	                for (int i = low; i <= high; i++) {
	                    temporary[i] = reference[i];
	    			}
                    return null;
                }
            };
        }

		/**
         * <p>
         * The {@code removeDuplicates} method} checks the validity of the merge sort
         * and removes from the reference array all but one of a set of references that
         * reference duplicate tuples.
         * </p>
         * 
         * @param reference - an array of references to the (x,y,z,w...) coordinates
         * @param p - the index of the most significant coordinate in the super key
         * @returns the address of the last element of the references array following duplicate removal
         */
		private static int removeDuplicates(final int[][] reference, final int p) {
			int end = 0;
        	for (int j = 1; j < reference.length; j++) {
        		int compare = superKeyCompare(reference[j], reference[j-1], p);
        		if (compare < 0) {
        			throw new RuntimeException( "merge sort failure: superKeyCompare(ref[" +
        					Integer.toString(j) + "], ref[" + Integer.toString(j-1) +
        					"], (" + Integer.toString(p) + ") = " + Integer.toString(compare) );
        		} else if (compare > 0) {
        			reference[++end] = reference[j];
        		}
        	}
			return end;
		}

        /**
         * <p>
         * The {@code removeDuplicatesWithThread} method returns a
         * {@link java.util.concurrent.Callable Callable} whose call() method executes the 
         * {@link KdNode#removeDuplicates removeDuplicates} method.
         * </p>
         * 
         * @param reference - an array of references to the (x,y,z,w...) coordinates
         * @param p - the index of the most significant coordinate in the super key
         * @returns the address of the last element of the references array following duplicate removal
         */
        private static Callable<Integer> removeDuplicatesWithThread(final int[][] reference, final int p) {
            
            return new Callable<Integer>() {
                @Override
                public Integer call() {
                    return removeDuplicates( reference, p);
                 }
            };
        }
        
		/**
		 * <p>
		 * Build a k-d tree by recursively partitioning the reference arrays and adding nodes to the tree.
		 * These arrays are permuted cyclically for successive levels of the tree in order that sorting use
		 * x, y, z, etc. as the most significant portion of the sorting or partitioning key.  The contents
		 * of the reference arrays are scrambled by each recursive partitioning.
		 * </p>
		 *
		 * @param references - arrays of references to the (x,y,z,w...) coordinates
		 * @param temporary - a scratch array into which to copy references
		 * @param start - the first element of the reference array a
		 * @param end - the last element of the reference array a
		 * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
		 * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
		 * @param depth - the depth in the k-d tree
		 * @returns the root of the k-d tree
		 */
		private static KdNode buildKdTree(final int[][][] references, final int[][] temporary,
				final int start, final int end,
				final ExecutorService executor, final int maximumSubmitDepth, final int depth) {

			final KdNode node;

			// The partition cycles as x, y, z, etc.
			final int p = depth % references.length;

			if (end == start) {

				// Only one reference was passed to this method, so store it at this level of the tree.
				node = new KdNode(references[0][start]);

			} else if (end == start + 1) {
				
				// Two references were passed to this method in sorted order, so store the start
				// element at this level of the tree and store the end element as the > child. 
				node = new KdNode(references[0][start]);
				node.gtChild = new KdNode(references[0][end]);
				
			} else if (end == start + 2) {
				
				// Three references were passed to this method in sorted order, so
				// store the median element at this level of the tree, store the start
				// element as the < child and store the end element as the > child.
				node = new KdNode(references[0][start + 1]);
				node.ltChild = new KdNode(references[0][start]);
				node.gtChild = new KdNode(references[0][end]);
				
			} else if (end > start + 2) {
				
				// Four or more references were passed to this method.  Partitioning of the other reference
				// arrays will occur about the median element of references[0].  Avoid overflow when
				// calculating the median.  Store the median element of references[0] in a new k-d node.
				int median = start + ((end - start) >> 1);
				if (median <= start || median >= end) {
					throw new RuntimeException("error in median calculation at depth = " + depth +
						" : start = " + start + "  median = " + median + "  end = " + end);
				}
				node = new KdNode(references[0][median]);

				// Copy a[0] to the temporary array before partitioning.
				for (int i = start; i <= end; i++) {
					temporary[i] = references[0][i];
				}

				// Sweep through each of the other reference arrays in its a priori sorted order
				// and partition it into "less than" and "greater than" halves by comparing
				// super keys.  Store the result from references[i] in references[i-1], thus
				// permuting the reference arrays.  Skip the element of references[i] that
				// references a point that equals the point that is stored in the new k-d node.
				int lower = -1;
				int upper = -1;
				int lowerSave = -1;
				int upperSave = -1;
				for (int i = 1; i < references.length; i++) {
					lower = start - 1;
					upper = median;
					for (int j = start; j <= end; j++) {
						
						// Process one reference array.  Compare once only.
						final int compare = superKeyCompare(references[i][j], node.point, p);
						if (compare < 0) {
							references[i-1][++lower] = references[i][j];
						} else if (compare > 0) {
							references[i-1][++upper] = references[i][j];
						}
					}

					// Check the new indices for the reference array.
					if (lower != median - 1) {
						throw new RuntimeException("incorrect range for lower at depth - " + depth +
								" : first = " + start + "  lower = " + lower + "  median = " + median);
					}

					if (upper != end) {
						throw new RuntimeException("incorrect range for upper at depth = " + depth +
								" : median = " + median + "  upper = " + upper + "  end = " + end);
					}
					if (i > 1 && lower != lowerSave) {
						throw new RuntimeException("lower = " + lower + "  !=  lowerSave = " + lowerSave);
					}

					if (i > 1 && upper != upperSave) {
						throw new RuntimeException("upper = " + upper + "  !=  upperSave = " + upperSave);
					}

					lowerSave = lower;
					upperSave = upper;
				}

				// Copy the temporary array to the last reference array to finish permutation.  The copies to
				// and from the temporary array produce the O((k+1)n log n)  term of the computational
				// complexity.  This term may be reduced to a O((k-1)n log n) term for (x,y,z) coordinates
				// by eliminating these copies and explicitly passing x, y, z and t (temporary) arrays to this
				// buildKdTree method, then copying t<-x, x<-y and y<-z, then explicitly passing x, y, t and z
				// to the next level of recursion.  However, this approach would sacrifice the generality
				// of sorting points of any number of dimensions because explicit calling parameters
				// would need to be passed to this method for each specific number of dimensions.
				for (int i = start; i <= end; i++) {
					references[references.length - 1][i] = temporary[i];
				}

				// Build the < branch with a child thread at as many levels of the tree as possible.
				// Create the child threads as high in the tree as possible for greater utilization.

				// Is a child thread available to build the < branch?
				if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

					// No, so recursively build the < branch of the tree with the current thread.
					node.ltChild = buildKdTree(references, temporary, start, lower, executor, maximumSubmitDepth, depth + 1);

					// Then recursively build the > branch of the tree with the current thread.
					node.gtChild = buildKdTree(references, temporary, median + 1, upper, executor, maximumSubmitDepth, depth + 1);
					
				} else {
					
					// Yes, a child thread is available, so recursively build the < branch with a child thread.
					final Future<KdNode> future = executor.submit( buildKdTreeWithThread(references, temporary,
							start, lower, executor, maximumSubmitDepth, depth + 1) );
					
					// And simultaneously, recursively build the > branch of the tree with the current thread.
					node.gtChild = buildKdTree(references, temporary, median + 1, upper, executor, maximumSubmitDepth, depth + 1);
					
					// Then get the result of building the < branch with the child thread.
					try {
						node.ltChild = future.get();
					} catch (Exception e) {
						throw new RuntimeException( "future exception: " + e.getMessage() );
					}
				}
				
			} else 	if (end < start) {
				
				// This is an illegal condition that should never occur, so test for it last.
				throw new RuntimeException("end < start");
				
			} else {
				
				// This final else block is added to keep the Java compiler from complaining.
				throw new RuntimeException("unknown configuration of  start and end");
			}
			
			return node;
		}

		/**
		 * <p>
		 * Return a {@link java.util.concurrent.Callable Callable} whose call() method executes the 
		 * {@link KdNode#buildKdTree buildKdTree} method.
		 * </p>
		 * 
		 * @param references - arrays of references to the (x,y,z,w...) coordinates
		 * @param temporary - a scratch array into which to copy references
		 * @param start - the first element of the reference array a
		 * @param end - the last element of the reference array a
		 * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
		 * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
		 * @param depth - the depth in the k-d tree
		 * @return a {@link KdNode}
		 */
		private static Callable<KdNode> buildKdTreeWithThread(final int[][][] references, final int[][] temporary,
				final int start, final int end,
				final ExecutorService executor, final int maximumSubmitDepth, final int depth) {
			
			return new Callable<KdNode>() {
				@Override
				public KdNode call() {
					return buildKdTree(references, temporary, start, end, executor, maximumSubmitDepth, depth);
				}
			};
		}

		/**
		 * <p>
		 * The {@code verifyKdTree} method checks that the children of each node of the k-d tree
		 * are correctly sorted relative to that node.
		 * </p>
		 * 
		 * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
		 * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
		 * @param depth - the depth in the k-d tree
		 * @return the number of nodes in the k-d tree
		 */
		private int verifyKdTree(final ExecutorService executor, final int maximumSubmitDepth, final int depth) {

			if (point == null) {
				throw new RuntimeException("point is null");
			}

			// The partition cycles by the number of coordinates.
			final int p = depth % point.length;

			if (ltChild != null) {
				if (ltChild.point[p] > point[p]) {
					throw new RuntimeException("node is > partition!");
				}
				if (superKeyCompare(ltChild.point, point, p) >= 0) {
					throw new RuntimeException("node is >= partition!");
				}
			}
			if (gtChild != null) {
				if (gtChild.point[p] < point[p]) {
					throw new RuntimeException("node is < partition!");
				}
				if (superKeyCompare(gtChild.point, point, p) <= 0) {
					throw new RuntimeException("node is <= partition!");
				}
			}
			
			// Count this node.
			int count = 1 ;

			// Search the < branch with a child thread at as many levels of the tree as possible.
			// Create the child thread as high in the tree as possible for greater utilization.

			// Is a child thread available to build the < branch?
			if (maximumSubmitDepth < 0 || depth > maximumSubmitDepth) {

				// No, so search the < branch with the current thread.
				if (ltChild != null) {
					count += ltChild.verifyKdTree(executor, maximumSubmitDepth, depth + 1);
				}

				// Then search the > branch with the current thread.
				if (gtChild != null) {
					count += gtChild.verifyKdTree(executor, maximumSubmitDepth, depth + 1);
				}
			} else {

				// Yes, so launch a child thread to search the < branch.
				Future<Integer> future = null;
				if (ltChild != null) {
					future = executor.submit(
							ltChild.verifyKdTreeWithThread(executor, maximumSubmitDepth, depth + 1) );
				}

				// And simultaneously search the > branch with the current thread.
				if (gtChild != null) {
					count += gtChild.verifyKdTree(executor, maximumSubmitDepth, depth + 1);
				}

				// If a child thread searched the < branch, get the result.
				if (future != null) {
					try {
						count += future.get();
					} catch (Exception e) {
						throw new RuntimeException( "future exception: " + e.getMessage() );
					}
				}
			}

			return count;
		}

		/**
		 * <p>
		 * The {@code verifyKdTreeWithThread} method returns a
		 * {@link java.util.concurrent.Callable Callable} whose call() method executes the 
		 * {@link KdNode#verifyKdTree verifyKdTree} method.
		 * </p>
		 * 
		 * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
		 * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
		 * @param depth - the depth in the k-d tree
		 * @return the number of nodes in the k-d tree
		 */
		private Callable<Integer> verifyKdTreeWithThread(final ExecutorService executor, final int maximumSubmitDepth,
				final int depth) {
			
			return new Callable<Integer>() {
				@Override
				public Integer call() {
					return verifyKdTree(executor, maximumSubmitDepth, depth);
				}
			};
		}

		/**
		 * <p>
		 * The {@code createKdTree} method builds a k-d tree from an int[][] of points,
		 * where the coordinates of each point are stored as an int[].
		 * </p>
		 *  
		 * @param coordinates - the int[][] of points
		 * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
		 * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
		 * @returns the root of the k-d tree
		 */
		public static KdNode createKdTree(int[][] coordinates, final ExecutorService executor,
				final int maximumSubmitDepth) {
			
			// Declare and initialize the reference arrays.  The number of dimensions may be
			// obtained from either coordinates[0].length or references.length.  The number
			// of points may be obtained from either coordinates.length or references[0].length.
            final List<Future<Void>> initializeFutures = new ArrayList<Future<Void>>();
            long initTime = System.currentTimeMillis();
			final int[][][] references = new int[coordinates[0].length][coordinates.length][];
			for (int i = 0; i < references.length; i++) {
			    // Initialize all dimensions in parallel if possible.
			    if (executor == null) {
			        initializeReference(coordinates, references[i], i);
			    } else {
			        Future<Void> future =
			        		executor.submit( initializeReferenceWithThread(coordinates, references[i], i) );
			        initializeFutures.add(future);
			    }
			}
			for (Future<Void> future : initializeFutures) {
			    try {
			        future.get();
			    } catch (Exception e) {
			        throw new RuntimeException( "future exception: " + e.getMessage() );
			    }
			}
			initTime = System.currentTimeMillis() - initTime;
			
			// Sort the reference arrays via hierarchical multi-threading if possible.
			// Include the time to allocate the temporary array in the measurement of sort time.
			long sortTime = System.currentTimeMillis();
			final int[][] temporary = new int[coordinates.length][];
			for (int i = 0; i < references.length; i++) {
				mergeSort(references[i], temporary, 0, coordinates.length - 1, i, executor, maximumSubmitDepth, 0);
			}
			sortTime = System.currentTimeMillis() - sortTime;

			// Remove references to duplicate tuples via one pass through each reference array,
			// for all dimensions in parallel if possible.  Any two reference arrays might preserve
			// non-identical tuples that have duplicate coordinates; hence, a subsequent test for
			// equality must employ the superKeyCompare() method instead of the == operator.
            final List<Future<Integer>> removeFutures = new ArrayList<Future<Integer>>();
			long removeTime = System.currentTimeMillis();
			final int[] end = new int[coordinates[0].length];
			for (int i = 0; i < references.length; i++) {
				// Remove duplicates in parallel on a per-dimension basis if possible.
				if (executor == null) {
					end[i] = removeDuplicates(references[i], i);
				} else {
			        Future<Integer> future =
			        		executor.submit( removeDuplicatesWithThread(references[i], i) );
			        removeFutures.add(future);
				}
			}
			for (int i = 0; i < removeFutures.size(); i++) {
				final Future<Integer> future = removeFutures.get(i);
			    try {
			        end[i]= future.get();
			    } catch (Exception e) {
			        throw new RuntimeException( "future exception: " + e.getMessage() );
			    }
			}
			removeTime = System.currentTimeMillis() - removeTime;
			
			// Check that the same number of references was removed from each reference array.
			for (int i = 0; i < end.length - 1; i++) {
				for (int j = i + 1; j < end.length; j++) {
					if (end[i] != end[j]) {
						throw new RuntimeException("reference removal error");
					}
				}
			}
			
			// Build the k-d tree via hierarchical multi-threading if possible.
			long kdTime = System.currentTimeMillis();
			final KdNode root = buildKdTree(references, temporary, 0, end[0], executor, maximumSubmitDepth, 0);
			kdTime = System.currentTimeMillis() - kdTime;
			
			// Verify the k-d tree and report the number of nodes.
			long verifyTime = System.currentTimeMillis();
			final int numberOfNodes = root.verifyKdTree(executor, maximumSubmitDepth, 0);
			verifyTime = System.currentTimeMillis() - verifyTime;
			System.out.println("Number of nodes = " + numberOfNodes);
			
			final double iT = (double) initTime / 1000.;
			final double sT = (double) sortTime / 1000.;
			final double rT = (double) removeTime / 1000.;
			final double kT = (double) kdTime / 1000.;
			final double vT = verifyTime / 1000.;
			System.out.printf("\ntotalTime = %.2f  initTime = %.2f  sortTime = %.2f"
					+ "  removeTime = %.2f  kdTime = %.2f  verifyTime = %.2f\n\n",
						iT + sT + rT + kT + vT, iT, sT, rT, kT, vT);
			
			// Return the root of the tree.
			return root;
		}
		
		/**
		 * <p>
		 * The {@code searchKdTree} method searches the k-d tree and finds the KdNodes
		 * that lie within a cutoff distance from a query node in all k dimensions.
		 * </p>
		 *
		 * @param query - the query point
		 * @param cut - the cutoff distance
		 * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
		 * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
		 * @param depth - the depth in the k-d tree
		 * @return a {@link java.util.List List}{@code <}{@link KdNode}{@code >}
		 * that contains the k-d nodes that lie within the cutoff distance of the query node
		 */
		private List<KdNode> searchKdTree(final int[] query, final int cut,
				final ExecutorService executor, final int maximumSubmitDepth, final int depth) {

			// The partition cycles as x, y, z, etc.
			final int p = depth % point.length;

			// If the distance from the query node to the k-d node is within the cutoff distance
			// in all k dimensions, add the k-d node to a list.
			final List<KdNode> result = new ArrayList<KdNode>();
			boolean inside = true;
			for (int i = 0; i < point.length; i++) {
				if (Math.abs(query[i] - point[i]) > cut) {
					inside = false;
					break;
				}
			}
			if (inside) {
				result.add(this);
			}

			// Search the < branch of the k-d tree if the partition coordinate of the query point minus
			// the cutoff distance is <= the partition coordinate of the k-d node.  The < branch must be
			// searched when the cutoff distance equals the partition coordinate because the super key
			// may assign a point to either branch of the tree if the sorting or partition coordinate,
			// which forms the most significant portion of the super key, shows equality.
			Future<List<KdNode>> future = null;
			if (ltChild != null) {
				if ( (query[p] - cut) <= point[p] ) {
					
					// Search the < branch with a child thread at as many levels of the tree as possible.
					// Create the child threads as high in the tree as possible for greater utilization.
					// If maxSubmitDepth == -1, there are no child threads.
					if (maximumSubmitDepth > -1 && depth <= maximumSubmitDepth) {
						future = executor.submit(
								ltChild.searchKdTreeWithThread(query, cut, executor, maximumSubmitDepth, depth + 1) );
					} else {
						result.addAll( ltChild.searchKdTree(query, cut, executor, maximumSubmitDepth, depth + 1) );
					}
				}
			}

			// Search the > branch of the k-d tree if the partition coordinate of the query point plus
			// the cutoff distance is >= the partition coordinate of the k-d node.  The < branch must be
			// searched when the cutoff distance equals the partition coordinate because the super key
			// may assign a point to either branch of the tree if the sorting or partition coordinate,
			// which forms the most significant portion of the super key, shows equality.
			if (gtChild != null) {
				if ( (query[p] + cut) >= point[p] ) {
					result.addAll( gtChild.searchKdTree(query, cut, executor, maximumSubmitDepth, depth + 1) );
				}
			}
			
			// If a child thread searched the < branch, get the result.
			if (future != null) {
				try {
					result.addAll( future.get() );
				} catch (Exception e) {
					throw new RuntimeException( "future exception: " + e.getMessage() );
				}
			}
			return result;
		}

		/**
		 * <p>
		 * The {@code searchKdTreeWithThread} method returns a
		 * {@link java.util.concurrent.Callable Callable} whose call() method executes the 
		 * {@link KdNode#searchKdTree searchKdTree} method.
		 * </p>
		 * 
		 * @param query - the query point
		 * @param cut - the cutoff distance
		 * @param executor - a {@link java.util.concurrent.ExecutorService ExecutorService}
		 * @param maximumSubmitDepth - the maximum tree depth at which a thread may be launched
		 * @param depth - the depth in the k-d tree
		 * @return a {@link java.util.List List}{@code <}{@link KdNode}{@code >}
		 * that contains the k-d nodes that lie within the cutoff distance of the query node
		 */
		public Callable<List<KdNode>> searchKdTreeWithThread(final int[] query, final int cut,
				final ExecutorService executor, final int maximumSubmitDepth, final int depth) {
			
			return new Callable<List<KdNode>>() {
				@Override
				public List<KdNode> call() {
					return searchKdTree(query, cut, executor, maximumSubmitDepth, depth);
				}
			};
		}

		/**
		 * <p>
		 * The {@code printKdTree} method prints the k-d tree "sideways" with the root at the left.
		 * </p>
		 * 
		 * @param depth - the depth in the k-d tree
		 */
		public void printKdTree(final int depth) {
			if (gtChild != null) {
				gtChild.printKdTree(depth+1);
			}
			for (int i = 0; i < depth; i++) {
				System.out.print("        ");
			}
			printTuple(point);
			System.out.println();
			if (ltChild != null) {
				ltChild.printKdTree(depth+1);
			}
		}

		/**
		 * <p>
		 * The {@code printTuple} method prints a tuple.
		 * </p>
		 * 
		 * @param p - the tuple
		 */
		public static void printTuple(final int[] p) {
			System.out.print("(");
			for (int i = 0; i < p.length; i++) {
				System.out.print(p[i]);
				if (i < p.length - 1) {
					System.out.print(", ");
				}
			}
			System.out.print(")");
		}
	}

	/**
	 * <p>
	 * The {@code randomIntegerInInterval} method creates a random int in the interval [min, max].
	 * See http://stackoverflow.com/questions/6218399/how-to-generate-a-random-number-between-0-and-1
	 * </p>
	 * 
	 * @param min - the minimum int value desired
	 * @param max - the maximum int value desired
	 * @returns a random int
	 */
	private static int randomIntegerInInterval(final int min, final int max) {
		return min + (int) ( Math.random() * (max - min) );
	}
	
	/**
	 * <p>
	 * Define a simple data set then build a k-d tree.
	 * </p>
	 */
	public static void main(String[] args) {

		// Set the defaults then parse the input arguments.
		int numPoints = 262144;
		int extraPoints = 100;
		int numDimensions = 3;
		int numThreads = 5;
		int searchDistance = 2000000000;
		int maximumNumberOfNodesToPrint = 5;
		
		for (int i = 0; i < args.length; i++) {
			if ( args[i].equals("-n") || args[i].equals("--numPoints") ) {
				numPoints = Integer.parseInt(args[++i]); 
				continue;
			}
			if ( args[i].equals("-x") || args[i].equals("--extraPoints") ) {
				extraPoints = Integer.parseInt(args[++i]); 
				continue;
			}
			if ( args[i].equals("-d") || args[i].equals("--numDimensions") ) {
				numDimensions = Integer.parseInt(args[++i]); 
				continue;
			}
			if ( args[i].equals("-t") || args[i].equals("--numThreads") ) {
				numThreads = Integer.parseInt(args[++i]); 
				continue;
			}
			if ( args[i].equals("-s") || args[i].equals("--searchDistance") ) {
				searchDistance = Integer.parseInt(args[++i]); 
				continue;
			}
			if ( args[i].equals("-p") || args[i].equals("--maximumNodesToPrint") ) {
				maximumNumberOfNodesToPrint = Integer.parseInt(args[++i]); 
				continue;
			}
			throw new RuntimeException("illegal command-line argument: " + args[i]);
		}
		
	    // Declare and initialize the coordinates array and initialize it with (x,y,z,w) tuples
	    // in the half-open interval [0, Integer.MAXIMUM_VALUE].  Don't use negative ints to avoid
		// overflow when comparing the random ints.  Create extraPoints-1 duplicate coordinates,
		// where extraPoints <= numPoints, to test the removal of duplicate points.
		extraPoints = (extraPoints <= numPoints) ? extraPoints: numPoints;
		final int[][] coordinates = new int[numPoints + extraPoints - 1][numDimensions];
		for (int i = 0; i < coordinates.length; i++) {
			for (int j = 0; j < coordinates[0].length; j++) {
				coordinates[i][j] = randomIntegerInInterval(0, Integer.MAX_VALUE);
			}
		}
		for (int i = 1; i < extraPoints; i++) {
			for (int j = 0; j < coordinates[0].length; j++) {
				coordinates[numPoints - 1 + i][j] = coordinates[numPoints - 1 - i][j];
			}
		}
		
		// Calculate the number of child threads to be the number of threads minus 1, then
		// calculate the maximum tree depth at which to launch a child thread.  Truncate
		// this depth such that the total number of threads, including the master thread, is
		// an integer power of 2, hence simplifying the launching of child threads by restricting
		// them to only the < branch of the tree for some number of levels of the tree.
		int n = 0;
		if (numThreads > 0) {
			while (numThreads > 0) {
				n++;
				numThreads >>= 1;
			}
			numThreads = 1 << (n - 1);
		} else {
			numThreads = 0;
		}
		final int childThreads = numThreads - 1;
		int maximumSubmitDepth = -1;
		if (numThreads < 2) {
			maximumSubmitDepth = -1; // The sentinel value -1 specifies no child threads.
		} else if (numThreads == 2) {
			maximumSubmitDepth = 0;
		} else {
			maximumSubmitDepth = (int) Math.floor( Math.log( (double) childThreads ) / Math.log(2.) );
		}
		System.out.println("\nNumber of child threads = " + childThreads + "  maximum submit depth = " + maximumSubmitDepth + "\n");
		
		// Create a fixed thread pool ExecutorService.
		ExecutorService executor = null;
		if (childThreads > 0) {
			try {
				executor = Executors.newFixedThreadPool(childThreads);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("executor exception " + e.getMessage());
			}
		}
		
		// Build the k-d tree.
		final KdNode root = KdNode.createKdTree(coordinates, executor, maximumSubmitDepth);

		// Search the k-d tree for all points that lie within a search distance of the first point.
		final int[] query = coordinates[0];
		long searchTime = System.currentTimeMillis();
		List<KdNode> kdNodes = root.searchKdTree(query, searchDistance, executor, maximumSubmitDepth, 0);
		searchTime = System.currentTimeMillis() - searchTime;
		final double sT = (double) searchTime / 1000.;
		System.out.printf("searchTime = %.2f\n", sT);
		System.out.print("\n" + kdNodes.size() + " nodes within " + searchDistance + " units of ");
		KdNode.printTuple(query);
		System.out.println(" in all dimensions.\n");
		if ( !kdNodes.isEmpty() ) {
			maximumNumberOfNodesToPrint = Math.min( maximumNumberOfNodesToPrint, kdNodes.size() );
			System.out.println("List of the first " + maximumNumberOfNodesToPrint + " k-d nodes within " +
											searchDistance + "-unit search distance follows:\n");
			for (int i = 0; i < maximumNumberOfNodesToPrint; i++) {
				KdNode node = kdNodes.get(i);
				KdNode.printTuple(node.point);
				System.out.println();
			}
		}
		
		// Shut down the ExecutorService.
		if (childThreads > 0) {
			executor.shutdown();
		}
	}
}
