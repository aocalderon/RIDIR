# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.16

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
CMAKE_COMMAND = /home/acald013/opt/cmake/cmake-3.16.4-Linux-x86_64/bin/cmake

# The command to remove a file.
RM = /home/acald013/opt/cmake/cmake-3.16.4-Linux-x86_64/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/acald013/RIDIR/Code/CGAL/RandomSegments2

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/acald013/RIDIR/Code/CGAL/RandomSegments2

# Include any dependencies generated for this target.
include CMakeFiles/random_segments.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/random_segments.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/random_segments.dir/flags.make

CMakeFiles/random_segments.dir/random_segments.cpp.o: CMakeFiles/random_segments.dir/flags.make
CMakeFiles/random_segments.dir/random_segments.cpp.o: random_segments.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/acald013/RIDIR/Code/CGAL/RandomSegments2/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/random_segments.dir/random_segments.cpp.o"
	/opt/rh/devtoolset-7/root/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/random_segments.dir/random_segments.cpp.o -c /home/acald013/RIDIR/Code/CGAL/RandomSegments2/random_segments.cpp

CMakeFiles/random_segments.dir/random_segments.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/random_segments.dir/random_segments.cpp.i"
	/opt/rh/devtoolset-7/root/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/acald013/RIDIR/Code/CGAL/RandomSegments2/random_segments.cpp > CMakeFiles/random_segments.dir/random_segments.cpp.i

CMakeFiles/random_segments.dir/random_segments.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/random_segments.dir/random_segments.cpp.s"
	/opt/rh/devtoolset-7/root/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/acald013/RIDIR/Code/CGAL/RandomSegments2/random_segments.cpp -o CMakeFiles/random_segments.dir/random_segments.cpp.s

# Object files for target random_segments
random_segments_OBJECTS = \
"CMakeFiles/random_segments.dir/random_segments.cpp.o"

# External object files for target random_segments
random_segments_EXTERNAL_OBJECTS =

random_segments: CMakeFiles/random_segments.dir/random_segments.cpp.o
random_segments: CMakeFiles/random_segments.dir/build.make
random_segments: /home/acald013/bin/lib/libmpfr.so
random_segments: /home/acald013/bin/lib/libgmp.so
random_segments: CMakeFiles/random_segments.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/acald013/RIDIR/Code/CGAL/RandomSegments2/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX executable random_segments"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/random_segments.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/random_segments.dir/build: random_segments

.PHONY : CMakeFiles/random_segments.dir/build

CMakeFiles/random_segments.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/random_segments.dir/cmake_clean.cmake
.PHONY : CMakeFiles/random_segments.dir/clean

CMakeFiles/random_segments.dir/depend:
	cd /home/acald013/RIDIR/Code/CGAL/RandomSegments2 && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/acald013/RIDIR/Code/CGAL/RandomSegments2 /home/acald013/RIDIR/Code/CGAL/RandomSegments2 /home/acald013/RIDIR/Code/CGAL/RandomSegments2 /home/acald013/RIDIR/Code/CGAL/RandomSegments2 /home/acald013/RIDIR/Code/CGAL/RandomSegments2/CMakeFiles/random_segments.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/random_segments.dir/depend

