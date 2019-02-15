default: main

main: main.o
	g++ -o parallel_quicksort main.o

main.o:
	g++ -c -std=c++17 -Wall -o main.o main.cpp


clean:
	rm *.o parallel_quicksort*
