default: main

main: main.o quicksort.o
	g++ -o parallel_quicksort main.o quicksort.o -pthread -luuid

main.o: main.cpp
	g++ -c -std=c++17 -Wall -Wextra -I. -o main.o main.cpp -g 

quicksort.o: quicksort.cpp quicksort.hpp
	g++ -c -std=c++17 -Wall -Wextra -I. -o quicksort.o quicksort.cpp -g 


clean:
	rm *.o parallel_quicksort*

cleanout:
	rm *.tmp *_out.csv
