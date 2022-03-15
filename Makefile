SOURCES := semisort.cpp
# Objs are all the sources, with .cpp replaced by .o
OBJS := $(SOURCES:.cpp=.o)

CFLAGS := -std=c++17
PBBFLAGS := -DHOMEGROWN -pthread

all: semisort

# Compile the binary 't' by calling the compiler with cflags, lflags, and any libs (if defined) and the list of objects.
semisort: $(OBJS) 
	$(CC) $(PBBFLAGS) $(CFLAGS) -o semisort $(OBJS) $(LFLAGS) $(LIBS)

# Get a .o from a .cpp by calling compiler with cflags and includes (if defined)
.cpp.o:
	$(CC) $(CFLAGS) $(PBBFLAGS) $(INCLUDES) -c $<