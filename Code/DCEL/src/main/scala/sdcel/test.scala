(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ amm -w test.scala
Script file not found: /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
Watching for changes to 2 files... (Ctrl-C to exit)
test.scala:1:11 expected (Semis | &"}" | end-of-input)
(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ amm -w test.scala
          ^
Watching for changes to 2 files... (Ctrl-C to exit)
  C-c C-c(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ 
(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ 
(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ 
(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ 
(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ 
(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ 
(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ ls
 DCELBuilder2.scala       Params.scala~               SweepLine2.scala~
 DCELBuilder2.scala~      PartitionReader.scala       SweepLine3.scala
 DCELMerger2.scala        PartitionReader.scala~      SweepLine3.scala~
 DCELMerger2.scala~       quadtree                   '#test.scala#'
 DCELPartitioner2.scala   SimplePartitioner2.scala    test.scala
 geometries               SimplePartitioner2.scala~
 Params.scala             SweepLine2.scala
(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ amm -w test.scala 
test.scala:1:11 expected (Semis | &"}" | end-of-input)
(base) and@and:~/RIDIR/Code/DCEL/src/main/scala/sdcel$ amm -w test.scala
          ^
Watching for changes to 2 files... (Ctrl-C to exit)
test.scala:1:1 expected end-of-input
package edu.ucr.dblab.sdcel
^
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
test.scala:4: not found: value edu
import edu.ucr.dblab.sdcel.geometries._
       ^
Compilation Failed
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
test
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
Half_edge(LINESTRING (1 1, 5 5)	A1	0	1	false)
Half_edge(LINESTRING (9 1, 5 5)	A2	0	1	false)
Half_edge(LINESTRING (9 9, 5 5)	A3	0	1	false)
Half_edge(LINESTRING (9 1, 5 5)	A4	0	1	false)
Half_edge(LINESTRING (5 5, 1 1)	A1	0	2	false)
Half_edge(LINESTRING (5 5, 9 9)	A2	0	2	false)
Half_edge(LINESTRING (5 5, 9 1)	A3	0	2	false)
Half_edge(LINESTRING (5 5, 1 1)	A4	0	2	false)
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
test.scala:41: value $ is not a member of StringContext
val res_32 = f.write(h.map{ x => $"${x.edge.toText}\t${x.data}\n"}.mkString(""))
                                 ^
Compilation Failed
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
Watching for changes to 2 files... (Ctrl-C to exit)
Compiling /home/and/RIDIR/Code/DCEL/src/main/scala/sdcel/test.scala
test.scala:41: not found: value x
val res_32 = f.write(hedges.map{ h => s"${x.edge.toText}\t${x.data}\n"}.mkString(""))
                                          ^
test.scala:41: not found: value x
val res_32 = f.write(hedges.map{ h => s"${x.edge.toText}\t${x.data}\n"}.mkString(""))
                                                            ^
test.scala:44: not found: value h
val res_34 = h.flatMap{}
             ^
Compilation Failed
Watching for changes to 2 files... (Ctrl-C to exit)
