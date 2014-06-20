#include <stdio.h>
#include <stdlib.h>
#include <time.h>

// Prototipos
int *crearVector(int);
void quickSort(int*, int, int);
int particion(int*, int, int);


int main (int argc, char *argv[]){
    int tamanioVector;
    int *punteroVector = NULL;
    time_t comienzoOrdenamiento, finalOrdenamiento;
    
    
    if (argc == 2){
        tamanioVector = atoi(argv[1]);
    }
    else{
        printf("ERROR >> El programa debe ser invocado con un parametro que indique el tamanio del vector.\n");
        return 1;
    }
    

    punteroVector = crearVector(tamanioVector);
    
    comienzoOrdenamiento = time(NULL);

    quickSort(punteroVector, 0, tamanioVector-1);
    
    finalOrdenamiento = time(NULL);

    printf("%d, %f\n",tamanioVector, difftime(finalOrdenamiento, comienzoOrdenamiento));

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
