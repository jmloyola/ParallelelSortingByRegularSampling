#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>


int *crearVector(int);
void quickSort(int*, int, int);
int particion(int*, int, int);

int main (int argc, char *argv[]){
	int numeroNodo, numeroProcesos;
    int tamanioVector;
    int *punteroVector = NULL;
        
	int j;

    int *sendcounts;
    int *displs;
    
    int *bufferRecepcion;

	int *localRegularSamples;
	
	int *gatheredRegularSamples;
	int *pivots;

    int remaining;
    int sum = 0;
    
    int w, p;
    
    time_t comienzoOrdenamiento, finalOrdenamiento;
    

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &numeroNodo);
    MPI_Comm_size(MPI_COMM_WORLD, &numeroProcesos);


    if (argc == 2){
        tamanioVector = atoi(argv[1]);
    }
    else{
        MPI_Finalize();
        printf("ERROR >> El programa debe ser invocado con un parametro que indique el tamanio del vector.");
        return 0;
    }
    

    if (numeroNodo == 0){
        punteroVector = crearVector(tamanioVector);
        
        /*
        printf("Vector sin ordenar:\n");

		for (j=0; j<tamanioVector; j++){
			printf("[%d] = %d\n", j, punteroVector[j]);
		}*/
    }
    
    
    comienzoOrdenamiento = time(NULL);


	sendcounts = malloc(sizeof(int)*numeroProcesos);
	displs = malloc(sizeof(int)*numeroProcesos);
	
	localRegularSamples = malloc(sizeof(int)*numeroProcesos);
    
    
    w = (int)(tamanioVector / (numeroProcesos * numeroProcesos));
    p = (int)(numeroProcesos / 2);
    
    remaining = tamanioVector % numeroProcesos;
    
    for (j=0; j < numeroProcesos; j++){
		sendcounts[j] = tamanioVector / numeroProcesos;
		if (remaining > 0){
			sendcounts[j]++;
			remaining--;
		}
		displs[j] = sum;
		sum += sendcounts[j];
	}    

    
    bufferRecepcion = (int*) malloc(sizeof(int)*sendcounts[numeroNodo]);

    MPI_Scatterv(punteroVector, sendcounts, displs, MPI_INT, bufferRecepcion, sendcounts[numeroNodo], MPI_INT, 0, MPI_COMM_WORLD);
       
    
    free(displs);
    
    
	quickSort(bufferRecepcion, 0, sendcounts[numeroNodo] - 1);
	
	int aux = 0;
	
	for (j=0; j<numeroProcesos; j++){
		localRegularSamples[j] = bufferRecepcion[aux];
		aux += w;
	}
	
	
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	
	
	pivots = malloc(sizeof(int)*(numeroProcesos-1));
	
	if (numeroNodo == 0){
		gatheredRegularSamples = malloc(sizeof(int)*numeroProcesos*numeroProcesos);
	}
	
	MPI_Gather(localRegularSamples, numeroProcesos, MPI_INT, gatheredRegularSamples, numeroProcesos, MPI_INT, 0, MPI_COMM_WORLD); //Recordar que el primer parametro es la direccion al dato a ser enviado, por ello debe ser un parametro por referencia.
		
	
	free(localRegularSamples);
	
	
	if (numeroNodo == 0){		
		quickSort(gatheredRegularSamples, 0, (numeroProcesos * numeroProcesos) - 1);
		
		aux = p - 1;/// EXPLICAR
		
		for (j=0; j<(numeroProcesos-1);j++){
			aux += numeroProcesos;
			pivots[j] = gatheredRegularSamples[aux];
		}
	}
	
	if (numeroNodo == 0){
		free(gatheredRegularSamples);
	}
	
	MPI_Bcast(pivots, numeroProcesos - 1, MPI_INT, 0, MPI_COMM_WORLD);
	
	
	MPI_Barrier(MPI_COMM_WORLD);



    int *sendCountsAllToAll = malloc(sizeof(int)*numeroProcesos);
    int *sendDisplAllToAll = malloc(sizeof(int)*numeroProcesos);
    int desplazamiento = 0;
    int cantidadItems = 0;
    int i = 0;
    for (j=0; j<(numeroProcesos-1);j++){
        sendDisplAllToAll[j] = desplazamiento;
        while ((i < sendcounts[numeroNodo]) && (bufferRecepcion[i] <= pivots[j])){ /// modifique esto para solucionar problema del sendCount
            cantidadItems++;
            i++;
        }
        sendCountsAllToAll[j] = cantidadItems;
        desplazamiento += cantidadItems;
        cantidadItems = 0;
    }
    
    
    free(pivots);
    

    sendDisplAllToAll[j] = desplazamiento;

    while (i < sendcounts[numeroNodo]){
        cantidadItems++;
        i++;
    }
    
    
    free(sendcounts);
    

    sendCountsAllToAll[j] = cantidadItems;


    int *recvCountsAllToAll = malloc(sizeof(int)*numeroProcesos);
    int *recvDisplAllToAll = malloc(sizeof(int)*numeroProcesos);

    for (j=0; j < numeroProcesos; j++){
		if (numeroNodo != j){
			MPI_Send(&sendCountsAllToAll[j], 1, MPI_INT, j, 1, MPI_COMM_WORLD);
		}
	}
	
	for (j=0; j < numeroProcesos; j++){
		if (numeroNodo != j){
			MPI_Recv(&recvCountsAllToAll[j], 1, MPI_INT, j, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		}
		else{
			recvCountsAllToAll[j] = sendCountsAllToAll[j];
		}
	}
	
	int desplazamientoRecvCount = 0;
	
	for (j=0; j < numeroProcesos; j++){
		recvDisplAllToAll[j] = desplazamientoRecvCount;
		
		desplazamientoRecvCount += recvCountsAllToAll[j];
	}
	
	
	int cantidadFinalRecivida = 0;
	
	for (j=0; j<numeroProcesos; j++){
		cantidadFinalRecivida += recvCountsAllToAll[j];
	}
	
	int *recvFinal = malloc(sizeof(int)*cantidadFinalRecivida);
	
	MPI_Alltoallv(bufferRecepcion, sendCountsAllToAll, sendDisplAllToAll, MPI_INT, recvFinal, recvCountsAllToAll, recvDisplAllToAll, MPI_INT, MPI_COMM_WORLD);
	
	
	free(recvDisplAllToAll);
	free(recvCountsAllToAll);
	free(sendDisplAllToAll);
	free(sendCountsAllToAll);
	free(bufferRecepcion);	
	
	
	MPI_Barrier(MPI_COMM_WORLD);
	
	
	
	quickSort(recvFinal, 0, cantidadFinalRecivida - 1);
	
	
	if (numeroNodo != 0){
		MPI_Send(&cantidadFinalRecivida, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
	}
	
	int *recvCountsGather = malloc(sizeof(int)*numeroProcesos);
	int *recvDisplGather = malloc(sizeof(int)*numeroProcesos);
	
	if (numeroNodo == 0){
		int desplazamientoGather = 0;
		
		recvCountsGather[0] = cantidadFinalRecivida;
		recvDisplGather[0] = desplazamientoGather;
		desplazamientoGather += recvCountsGather[0];
		
		for (j=1; j<numeroProcesos; j++){
			MPI_Recv(&recvCountsGather[j], 1, MPI_INT, j, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE); 
			recvDisplGather[j] = desplazamientoGather;
			desplazamientoGather += recvCountsGather[j];
		}
	}
	
    
    
    MPI_Gatherv(recvFinal, cantidadFinalRecivida, MPI_INT, punteroVector, recvCountsGather, recvDisplGather, MPI_INT, 0, MPI_COMM_WORLD);
	
	
	free(recvDisplGather);
	free(recvCountsGather);
	free(recvFinal);
	

	/*
	if (numeroNodo == 0){
		printf("\n\nVector ordenado:\n\n");

		for (j=0; j<tamanioVector; j++){
			printf("[%d] = %d\n", j, punteroVector[j]);
		}
	}*/
	
	if (numeroNodo == 0){
		free(punteroVector);
	}
	
	
	finalOrdenamiento = time(NULL);

    double tiempoTranscurrido = difftime(finalOrdenamiento, comienzoOrdenamiento);

    if (numeroNodo != 0){
        MPI_Send(&tiempoTranscurrido, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    }    

    if (numeroNodo == 0){
        double *todosTiemposTranscurridos = malloc(sizeof(double)*numeroProcesos);

        todosTiemposTranscurridos[0] = tiempoTranscurrido;

        for (j=1; j<numeroProcesos; j++){
            MPI_Recv(&todosTiemposTranscurridos[j], 1, MPI_DOUBLE, j, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }

        printf("%d, %d",numeroProcesos, tamanioVector);

        for (j=0; j<numeroProcesos; j++){
            printf(", %f",todosTiemposTranscurridos[j]);
        }
        printf("\n");
    }	
	
    MPI_Finalize(); 
        
    return 1; 
}


int *crearVector (int tamanio){
    int *punteroVector = NULL;
    int i;

    if (tamanio <= 0){
        printf("ERROR >> El tamanio del vector debe ser mayor a cero.\n");
        return NULL;
    }
    else{
        punteroVector = (int*) calloc(tamanio, sizeof(int));

        if (punteroVector == NULL){
            printf("ERROR >> No se pudo crear el vector.\n");
            return NULL;
        }
        else{
            srand (time(NULL));

            for (i=0; i<tamanio; i++){
                punteroVector[i] = rand() % (tamanio + 1);
            }
            return punteroVector;
        }
    }
}

void quickSort(int *punteroVector, int izq, int der){
    int j;

    if (izq < der){
        j = particion(punteroVector, izq, der);
        quickSort(punteroVector, izq, j-1);
        quickSort(punteroVector, j+1, der);
    }
}

int particion(int *punteroVector, int izq, int der){
    int pivot, i, j, t;

    pivot = punteroVector[izq];
    i = izq;
    j = der + 1;

    while (1){
        do ++i; while(punteroVector[i] <= pivot && i <= der);
        do --j; while(punteroVector[j] > pivot);
        if (i >= j) break;
        t = punteroVector[i];
        punteroVector[i] = punteroVector[j];
        punteroVector[j] = t;
    }
    t = punteroVector[izq];
    punteroVector[izq] = punteroVector[j];
    punteroVector[j] = t;
    return j;
}
