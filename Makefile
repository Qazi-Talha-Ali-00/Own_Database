CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -I.

SRCS = main.cpp Database.cpp Table.cpp Record.cpp
OBJS = $(SRCS:.cpp=.o)

TARGET = minidb

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)