psrs: psrs.c
	 mpicc -o psrs psrs.c -Wall

quickSort: quickSort.c
	gcc -o quickSort quickSort.c -Wall
