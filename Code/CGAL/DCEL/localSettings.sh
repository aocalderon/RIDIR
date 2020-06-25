#!/bin/bash

rm -fR CMakeFiles/
rm CMake*
rm cmake_install.cmake 
rm Makefile 

/opt/CGAL-5.0.2/scripts/cgal_create_CMakeLists
cmake -DCMAKE_BUILD_TYPE=Release -DCGAL_DIR=/opt/CGAL-5.0.2/ .
