default: main

main: main.o quicksort.o
	g++ -o parallel_quicksort main.o quicksort.o -pthread

main.o:
	g++ -c -std=c++17 -Wall -Wextra -I. -o main.o main.cpp -g 

quicksort.o:
	g++ -c -std=c++17 -Wall -Wextra -I. -o quicksort.o quicksort.cpp -g 


clean:
	rm *.o parallel_quicksort*
