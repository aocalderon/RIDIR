#!/bin/bash

export BOOST_ROOT=/home/acald013/bin/
source /opt/rh/devtoolset-7/enable

rm -fR CMakeFiles/
rm CMake*
rm cmake_install.cmake 
rm Makefile 

~/opt/CGAL-5.0.2/scripts/cgal_create_CMakeLists
cmake -DCMAKE_BUILD_TYPE=Release -DCGAL_DIR=/home/acald013/opt/CGAL-5.0.2/ .
