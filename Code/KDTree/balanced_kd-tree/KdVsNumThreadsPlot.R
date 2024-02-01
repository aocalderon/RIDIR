# Copyright (c) 2015, Russell A. Brown
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
#    may be used to endorse or promote products derived from this software without
#    specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSEARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
#
# Note: due to an incompatibility between R and Mac OS X Yosemite, the "Source script
# or load data in R" button of the R Console may not work correctly and result in the
# "spinning rainbow of death".  If so, type the source command in the R console:
# > source("path_to_R_program_file")
#
# Fit execution time to the T = Ts + T1/Q + Mc*(Q-1) model,
# where T is the execution time, Q is the number of threads,
# Ts is the Amdahl serial execution time, T1 is the Amdahl
# parallelizable execution time for one thread, and Mc is
# a per-thread execution time penalty due to memory contention.
# by using averages to fit T1 then least squares for Ts and Mc.

# Read in the execution-time vs. number-of-threads data from the file
fname <- "KdVsNumThreadsData.txt"	# data file name
dir<-"path_to_data_directory"	# directory path
fullname <- paste(dir, fname, sep = "/")
if (is.character(fullname) != TRUE) { print("Bad file name") }
input <- read.table(fullname, header=TRUE)

# Find the minimum number of threads and their indices.
i <- 1
minT <- input$T_N[1]
minI <- 1
while (i <= length(input$T_N)) {
    if (input$T_N[i] < minT) {
        minT <- input$T_N[i]
        minI <- i
    }
    i <- i + 1
}

# Average the kdTree build times for the minimum number of threads.
i <- 1
minN <- 0
minS <- 0.
while (i <= length(input$T_N)) {
    if (input$T_N[i] == minT) {
        minS <- minS + input$S_N[i]
        minN <- minN + 1
    }
    i <- i + 1
}

# Calculate T1 from the average build time.
T1 <- minS / minN

# Define the Amdahl function of t with a memory-contention term.
calcAmdahl <- function(t, ts, t1, mc) {
	ts + (t1 / t) + mc * (t - 1.)
}

# Define X as the number of threads and Y as the execution time
X <- input$T_N
Y <- input$S_N

# Perform a linear least-squares fit to obtain Ts and Mc.
sX <- 0.0
sY <- 0.0
sX2 <- 0.0
sY2 <- 0.0
sXY <- 0.0
n <- 0.0
i <- 1
while (i <= length(input$T_N)) {
	x <- X[i] - 1.
	y <- Y[i] - (T1 / X[i])
	sX <- sX + x
	sY <- sY + y
	sX2 <- sX2 + x*x
	sY2 <- sY2 + y*y
	sXY <- sXY + x*y
	n <- n + 1
	i <- i + 1
}
dmat <- matrix(c(sX2, sX, sX, n), nrow=2, ncol=2)
mmat <- matrix(c(sXY, sX, sY, n), nrow=2, ncol=2)
bmat <- matrix(c(sX2, sXY, sX, sY), nrow=2, ncol=2)
Mc <- det(mmat) / det(dmat)
Ts <- det(bmat) / det(dmat)
r <- (n * sXY - sX * sY) / sqrt((n * sX2 - sX * sX) * (n * sY2 - sY * sY))

# Calculate xRange and yRange
xRange <- c( min(input$T_N), max(input$T_N) )
yRange <- c( min(input$S_N), max(input$S_N) )

# Calculate xSeq in order to step x in smaller increments than the default 1.
xSeq <- seq(  min(input$T_N), max(input$T_N), 0.1 )

# Calculate ySeq in the same manner so that it is the same length as xSeq,
# then recalculate each element of ySeq using the quasi-Amdahl function.
ySeq <- seq(  min(input$T_N), max(input$T_N), 0.1 )
i <- 1
while ( i <= length(xSeq) ) {
	ySeq[i] <- calcAmdahl(xSeq[i], Ts, T1, Mc)
	i <- i + 1
}

# Plot all the results
plot(x=xSeq, y=ySeq, xlim=xRange, ylim=yRange, type="l", lty="dashed", lwd=1, ylab="Total k-d Tree-Building Time (s)", xlab="Number of Threads")
title("Total k-d Tree-Building Time vs. Number of Threads")
points(input$T_N, input$S_N)
legend("top", legend=eval(parse(text=sprintf(
   "expression(t[1] == %.2f, t[S] == %.2f, m[C] == %.2f, r == %.4f)",
   T1, Ts, Mc, r))), ncol=2)
