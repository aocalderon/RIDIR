# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.13

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/and/RIDIR/Code/CGAL/DCEL

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/and/RIDIR/Code/CGAL/DCEL

# Include any dependencies generated for this target.
include CMakeFiles/dcel.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/dcel.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/dcel.dir/flags.make

CMakeFiles/dcel.dir/dcel.cpp.o: CMakeFiles/dcel.dir/flags.make
CMakeFiles/dcel.dir/dcel.cpp.o: dcel.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/and/RIDIR/Code/CGAL/DCEL/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/dcel.dir/dcel.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/dcel.dir/dcel.cpp.o -c /home/and/RIDIR/Code/CGAL/DCEL/dcel.cpp

CMakeFiles/dcel.dir/dcel.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/dcel.dir/dcel.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/and/RIDIR/Code/CGAL/DCEL/dcel.cpp > CMakeFiles/dcel.dir/dcel.cpp.i

CMakeFiles/dcel.dir/dcel.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/dcel.dir/dcel.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/and/RIDIR/Code/CGAL/DCEL/dcel.cpp -o CMakeFiles/dcel.dir/dcel.cpp.s

# Object files for target dcel
dcel_OBJECTS = \
"CMakeFiles/dcel.dir/dcel.cpp.o"

# External object files for target dcel
dcel_EXTERNAL_OBJECTS =

dcel: CMakeFiles/dcel.dir/dcel.cpp.o
dcel: CMakeFiles/dcel.dir/build.make
dcel: /usr/lib/x86_64-linux-gnu/libmpfr.so
dcel: /usr/lib/x86_64-linux-gnu/libgmp.so
dcel: CMakeFiles/dcel.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/and/RIDIR/Code/CGAL/DCEL/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable dcel"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/dcel.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/dcel.dir/build: dcel

.PHONY : CMakeFiles/dcel.dir/build

CMakeFiles/dcel.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/dcel.dir/cmake_clean.cmake
.PHONY : CMakeFiles/dcel.dir/clean

CMakeFiles/dcel.dir/depend:
	cd /home/and/RIDIR/Code/CGAL/DCEL && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/and/RIDIR/Code/CGAL/DCEL /home/and/RIDIR/Code/CGAL/DCEL /home/and/RIDIR/Code/CGAL/DCEL /home/and/RIDIR/Code/CGAL/DCEL /home/and/RIDIR/Code/CGAL/DCEL/CMakeFiles/dcel.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/dcel.dir/depend

