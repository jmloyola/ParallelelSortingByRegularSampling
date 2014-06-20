#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>

// Prototipos
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

	// Se controla que el programa sea invocado con un argumento que indique la cantidad de elementos a ordenar.
	// En caso de encontrarlo asigna la variable correspondiente. Si no existe este parametro se muestra un mensaje de error y se termina la ejecucion del programa.
    if (argc == 2){
        tamanioVector = atoi(argv[1]);
    }
    else{
        MPI_Finalize();
        printf("ERROR >> El programa debe ser invocado con un parametro que indique el tamanio del vector.\n");
        return 1;
    }
    
	// El proceso maestro crea el vector de elementos a ordenar.
    if (numeroNodo == 0){
        punteroVector = crearVector(tamanioVector);
    }
    
    
    /// --------------------------------------------------------
    /// Comienza Fase 1 de Parallel Sorting by Regular Sampling
    /// --------------------------------------------------------
    // Se toma el tiempo en el que comienza efectivamente el algortimo psrs.
    comienzoOrdenamiento = time(NULL);

	sendcounts = malloc(sizeof(int)*numeroProcesos);
	displs = malloc(sizeof(int)*numeroProcesos);
	
	localRegularSamples = malloc(sizeof(int)*numeroProcesos);  
    
    w = (int)(tamanioVector / (numeroProcesos * numeroProcesos));
    p = (int)(numeroProcesos / 2);
    
    // Realizo el calculo de la cantidad de items que debo distribuir a cada proceso.
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

	// Notar que se utiliza la comunicacion en grupo Scatterv ya que las porciones a distribuir entre los procesos pueden no ser del mismo tamanio.
    MPI_Scatterv(punteroVector, sendcounts, displs, MPI_INT, bufferRecepcion, sendcounts[numeroNodo], MPI_INT, 0, MPI_COMM_WORLD);    
    
    free(displs);
    
	quickSort(bufferRecepcion, 0, sendcounts[numeroNodo] - 1);
	
	// Notar que comienza en '0' y no en '1' como en el paper debido a que en C los arreglos comienzan en la posicion '0'.
	int aux = 0;
	
	// Cada proceso construye el vector con las muestras regulares locales a enviar al proceso maestro
	for (j=0; j<numeroProcesos; j++){
		localRegularSamples[j] = bufferRecepcion[aux];
		aux += w;
	}
	/// --------------------------------------------------------
	/// Fin Fase 1 de Parallel Sorting by Regular Sampling
	/// --------------------------------------------------------
	
	
	/// --------------------------------------------------------
	/// Comienza Fase 2 de Parallel Sorting by Regular Sampling
	/// --------------------------------------------------------
	pivots = malloc(sizeof(int)*(numeroProcesos-1));
	
	if (numeroNodo == 0){
		gatheredRegularSamples = malloc(sizeof(int)*numeroProcesos*numeroProcesos);
	}
	
	MPI_Gather(localRegularSamples, numeroProcesos, MPI_INT, gatheredRegularSamples, numeroProcesos, MPI_INT, 0, MPI_COMM_WORLD); //Recordar que el primer parametro es la direccion al dato a ser enviado, por ello debe ser un parametro por referencia.
		
	free(localRegularSamples);
	
	if (numeroNodo == 0){		
		quickSort(gatheredRegularSamples, 0, (numeroProcesos * numeroProcesos) - 1);
		
		// Notar que se resta uno a 'p' ya que la primera posicion del arreglo en C no se encuentra en '1' (como se considera en el paper) sino en '0'.
		aux = p - 1;
		
		// El proceso maestro construye el vector con los pivotes a enviar a todos los procesos.
		for (j=0; j<(numeroProcesos-1);j++){
			aux += numeroProcesos;
			pivots[j] = gatheredRegularSamples[aux];
		}
	}
	
	if (numeroNodo == 0){
		free(gatheredRegularSamples);
	}
	
	MPI_Bcast(pivots, numeroProcesos - 1, MPI_INT, 0, MPI_COMM_WORLD);
	/// --------------------------------------------------------
	/// Fin Fase 2 de Parallel Sorting by Regular Sampling
	/// --------------------------------------------------------


	/// --------------------------------------------------------
	/// Comienza Fase 3 de Parallel Sorting by Regular Sampling
	/// --------------------------------------------------------
    int *sendCountsAllToAll = malloc(sizeof(int)*numeroProcesos);
    int *sendDisplAllToAll = malloc(sizeof(int)*numeroProcesos);
    int desplazamiento = 0;
    int cantidadItems = 0;
    int i = 0;
    
    // Cada proceso determina cuantos items debe enviar a cada proceso y el desplazamiento de la tabla. Utilizo los pivotes para ello.
    for (j=0; j<(numeroProcesos-1);j++){
        sendDisplAllToAll[j] = desplazamiento;
        while ((i < sendcounts[numeroNodo]) && (bufferRecepcion[i] <= pivots[j])){
            cantidadItems++;
            i++;
        }
        sendCountsAllToAll[j] = cantidadItems;
        desplazamiento += cantidadItems;
        cantidadItems = 0;
    }
    
    free(pivots);

	// Notar que en la iteracion anterior no se consideran los items que son mayores al ultimo pivot. Por ello se calculo aqui.
    sendDisplAllToAll[j] = desplazamiento;

    while (i < sendcounts[numeroNodo]){
        cantidadItems++;
        i++;
    }
    
    free(sendcounts);

    sendCountsAllToAll[j] = cantidadItems;
    
    // Como cada proceso conoce cuanto debe enviar a cada proceso pero no conoce cuantos items va a recibir de cada proceso, todos los procesos deben informar cuanto enviaran a cada uno.

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
	/// --------------------------------------------------------
	/// Fin Fase 3 de Parallel Sorting by Regular Sampling
	/// --------------------------------------------------------
	
	
	/// --------------------------------------------------------
	/// Comienza Fase 4 de Parallel Sorting by Regular Sampling
	/// --------------------------------------------------------
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
		
		// Notar que la iteracion comienza en 1, ya que el proceso maestro ya tiene la informacion relevante.
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
	
	if (numeroNodo == 0){
		free(punteroVector);
	}
	
	// Se toma el tiempo en el que termina efectivamente el algortimo psrs.
	finalOrdenamiento = time(NULL);
	/// --------------------------------------------------------
	/// Fin Fase 4 de Parallel Sorting by Regular Sampling
	/// --------------------------------------------------------
	
	// Cada proceso realiza el calculo del tiempo transcurrido.
    double tiempoTranscurrido = difftime(finalOrdenamiento, comienzoOrdenamiento);

	// Todos los procesos excepto el maestro envian un mensaje al maestro con el tiempo transcurrido.
    if (numeroNodo != 0){
        MPI_Send(&tiempoTranscurrido, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    }    

	// El proceso maestro recibe los tiempos transcurridos de los demas procesos.
    if (numeroNodo == 0){
        double *todosTiemposTranscurridos = malloc(sizeof(double)*numeroProcesos);
		
		// El proceso maestro conoce localmente cuanto tardo el mismo. Por ello dicha entrada de la tabla es completada directamente con esta informacion.
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
        
    return 0; 
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
