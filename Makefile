default: main

main: main.o quicksort.o
	g++ -o parallel_quicksort main.o

main.o:
	g++ -c -lpthread -std=c++17 -Wall -Wextra -I. -o main.o main.cpp -g 

quicksort.o:
	g++ -c -pthread -std=c++17 -Wall -Wextra -I. -o quicksort.o quicksort.cpp -g 


clean:
	rm *.o parallel_quicksort*
