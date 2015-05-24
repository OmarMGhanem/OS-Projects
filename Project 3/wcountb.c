#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>

#define MAX_N 20
#define MIN_N 1
#define MAX_R 50
#define MIN_R 1
#define MAX_BUFFER 10000
#define MIN_BUFFER 10
#define MAX_LENGTH 256

// TODO: buffer finishes, wordArray size (realloc), merger, check leaks

typedef struct{
	char **buf;
	int writeIndex;
	int readIndex;
	int count;
	bool isDone;
	pthread_mutex_t lock;
	pthread_cond_t spaceAvailable; // less
	pthread_cond_t itemAvailable; // more
	
} buffer_t;

// global variables
int N;
int R;
int bufferSize;

buffer_t* **buffers; // buffers between mapper & reducer
buffer_t* *rbuffers; // buffers between reducer & merger
pthread_t *tidMapper;
pthread_t *tidReducer;

// takes a char array and converts each char into int and finds the sum
int hash(char *w){
    int size = strlen(w);
    int i, c = 0;
    for(i = 0; i < size; i++){
        c += w[i];
    }
    return c;
}

// sorting strings function adapted to qsort, inspired by linux man
int cmpstring(const void *w1, const void *w2){
    return strcmp(*(char**)w1, *(char**)w2);
}

// mapper thread function
void *mapper(void *arg){
	
	char *fileName;
    fileName = (char*)arg;
	FILE *inputFile;
    
    int j = 0, k = 0;
    char word[MAX_LENGTH];
    
    // correct thread selection
	pthread_t id = pthread_self();
    while(pthread_equal(id, tidMapper[j]) == 0){
		j++; 
    }
	
	inputFile = fopen(fileName, "r");
	for(k = 0; k < R; k++){
	    buffers[j][k]->buf = (char**)malloc(bufferSize * sizeof(char*));
	}
	
    // read input files then partition words into buffers according to their hash results
    while(fscanf(inputFile, "%255s", word) != EOF){
		
        int i;
        i = hash(word) % R;
		
		pthread_mutex_lock(&(buffers[j][i]->lock));
		// critical section entered
		while(buffers[j][i]->count == bufferSize){
			pthread_cond_wait(&(buffers[j][i]->spaceAvailable), &(buffers[j][i]->lock));
		}
		
		buffers[j][i]->buf[buffers[j][i]->writeIndex] = (char*)malloc(strlen(word) + 1);
		strcpy(buffers[j][i]->buf[buffers[j][i]->writeIndex], word);
		buffers[j][i]->writeIndex = ((buffers[j][i]->writeIndex + 1) % bufferSize);
		buffers[j][i]->count++;
		
		pthread_cond_signal(&(buffers[j][i]->itemAvailable));
		
		pthread_mutex_unlock(&(buffers[j][i]->lock));
		// critical section exited
    }
	
	// sync threads
	for(k = 0; k < R; k++){
		pthread_mutex_lock(&(buffers[j][k]->lock));
		// critical section entered
		while(buffers[j][k]-> count != 0){
			pthread_cond_wait(&(buffers[j][k]->spaceAvailable), &(buffers[j][k]->lock));
			
		}
		buffers[j][k]->isDone = true;
		pthread_cond_signal(&(buffers[j][k]->itemAvailable));
		pthread_mutex_unlock(&(buffers[j][k]->lock));
		// critical section exited
	}
	
    fclose(inputFile);	
	
	return NULL;
}

// reducer thread function
void *reducer(void *arg){
    
    int i = 0, j = 0, k = 0; 
    int sameCount = 0;
	int count = 0;
    
    // correct thread selection
	pthread_t id = pthread_self();
    while(pthread_equal(id, tidReducer[j]) == 0){
		j++;
    }
    
    // allocate buf array
	rbuffers[j]->buf = (char**)malloc(bufferSize * sizeof(char*)); // does this need realloc?
	
	// allocate word array
	char **wordArray = (char**)malloc(250000 * sizeof(char*));
	
	// read from mapper and write to array for sorting
	for(i = 0; i < N; i++){
		pthread_mutex_lock(&(buffers[i][j]->lock));
		// critical section entered
		while(buffers[i][j]->count == 0 && buffers[i][j]->isDone == false){
                        pthread_cond_wait(&(buffers[i][j]->itemAvailable), &(buffers[i][j]->lock));
                }
        if(buffers[i][j]->count == 0 && buffers[i][j]->isDone == true){
            pthread_mutex_unlock(&(buffers[i][j]->lock));
        }
		else{
			
			while(buffers[i][j]->count > 0){
		    
				wordArray[count] = (char*)malloc(strlen(buffers[i][j]->buf[buffers[i][j]->readIndex]) + 1);
				//wordArray = (char**)realloc(wordArray, (count * sizeof(char*)));
			
				strcpy(wordArray[count], buffers[i][j]->buf[buffers[i][j]->readIndex]);
				count++;
			
				buffers[i][j]->readIndex = ((buffers[i][j]->readIndex + 1) % bufferSize);
				buffers[i][j]->count--;
				//buffers[i][j]->buf[buffers[i][j]->readIndex] = NULL;
				free(buffers[i][j]->buf[buffers[i][j]->readIndex]);
			}
			
			pthread_cond_signal(&(buffers[i][j]->spaceAvailable));
			pthread_mutex_unlock(&(buffers[i][j])->lock);
		}		
	}
	
	qsort(wordArray, count, sizeof(char*), cmpstring);
	
	// write to merger
	char* currWord = NULL;
	for(k = 0; k < count; k++){
		if(currWord == NULL){
				currWord = wordArray[k];
				sameCount = 1;
		}
		else{
			if(strcmp(currWord, wordArray[k]) == 0){
				sameCount++;
			}
			else{
				pthread_mutex_lock(&(rbuffers[j])->lock);
				// critical section entered
				while(rbuffers[j]->count == bufferSize){
					pthread_cond_wait(&(rbuffers[j]->spaceAvailable), &(rbuffers[j]->lock));
				}
				
				rbuffers[j]->buf[rbuffers[j]->writeIndex] = (char*)malloc(strlen(currWord) + 10); // needs more size because of int
				
				sprintf(rbuffers[j]->buf[rbuffers[j]->writeIndex], "%s %d", currWord, sameCount);  
			
				rbuffers[j]->writeIndex = (rbuffers[j]->writeIndex+1) % bufferSize;
				rbuffers[j]->count++;
				
				pthread_cond_signal(&(rbuffers[j]->itemAvailable));
				pthread_mutex_unlock(&(rbuffers[j])->lock);
				// critical section exited
				currWord = wordArray[k];
				sameCount = 1;
			}
		}        
	}    
	if(currWord != NULL){
		pthread_mutex_lock(&(rbuffers[j])->lock);
		// critical section entered
		while(rbuffers[j]->count == bufferSize){
				pthread_cond_wait(&(rbuffers[j]->spaceAvailable), &(rbuffers[j]->lock));
		}
		
		rbuffers[j]->buf[rbuffers[j]->writeIndex] = (char*)malloc(strlen(currWord) + 10); // needs more size because of int
		
		sprintf(rbuffers[j]->buf[rbuffers[j]->writeIndex], "%s %d", currWord, sameCount);
		
		rbuffers[j]->writeIndex = (rbuffers[j]->writeIndex+1) % bufferSize;
		rbuffers[j]->count++;
		
		pthread_cond_signal(&(rbuffers[j]->itemAvailable));
		pthread_mutex_unlock(&(rbuffers[j])->lock);
		// critical section exited
	}
	
	// deallocs
	for(k = 0; k < count; k++){
		free(wordArray[k]);
	}
	free(wordArray);
	
	return NULL;
}
int main(int argc, char **argv){
    
    // main thread, also merger thread
    
    N = atoi(argv[1]); // N
    R = atoi(argv[2]); // R
	bufferSize = atoi(argv[argc - 1]); // buffer size
    int error = 0, i, j;
    
    FILE* fileFinal = fopen(argv[argc - 2], "w+");
	
    // routine checks
    if(N > MAX_N || N < MIN_N){
        perror("Incorrect value of N!\n");
        exit(EXIT_FAILURE);
    }
    if(R > MAX_R || R < MIN_R){
        perror("Incorrect value of N!\n");
        exit(EXIT_FAILURE);
    }
	if(bufferSize < MIN_BUFFER || bufferSize > MAX_BUFFER){
		perror("Incorrect buffer size!\n");
		exit(EXIT_FAILURE);
	}
	
	tidMapper = (pthread_t*)malloc(N * sizeof(pthread_t));
    tidReducer = (pthread_t*)malloc(R * sizeof(pthread_t));
	
	// allocate 2d struct pointer array
	buffers = (buffer_t***)malloc(N * sizeof(buffer_t**));
	for(i = 0; i < N; i++){
		buffers[i] = (buffer_t**)malloc(R * sizeof(buffer_t*));
		for(j = 0; j < R; j++){
			buffers[i][j] = (buffer_t*)malloc(sizeof(buffer_t));
		}
	}
	
	// allocate 1d struct pointer array
	rbuffers = (buffer_t**)malloc(R * sizeof(buffer_t*));
	for(i = 0; i < R; i++){
		rbuffers[i] = (buffer_t*)malloc(sizeof(buffer_t));
	}
	
	// basic struct inits
	for(i = 0; i < N; i++){
		for(j = 0; j < R; j++){
			(buffers[i][j])->writeIndex = 0;
			(buffers[i][j])->readIndex = 0;
			(buffers[i][j])->count = 0;
			(buffers[i][j])->isDone = false;
		}
	}
	for(i = 0; i < R; i++){
		(rbuffers[i])->writeIndex = 0;
		(rbuffers[i])->readIndex = 0;
		(rbuffers[i])->count = 0;
		(rbuffers[i])->isDone = false;
	}
	
	// mutex initialization
	for(i = 0; i < N; i++){
		for(j = 0; j < R; j++){
			pthread_mutex_init(&(buffers[i][j]->lock), NULL);
		}
	}
	for(i = 0; i < R; i++){
		pthread_mutex_init(&(rbuffers[i]->lock), NULL);
	}
	
	// condition initialization
	for(i = 0; i < N; i++){
		for(j = 0; j < R; j++){
			pthread_cond_init(&(buffers[i][j]->itemAvailable), NULL);
			pthread_cond_init(&(buffers[i][j]->spaceAvailable), NULL);
		}
	}
	for(i = 0; i < R; i++){
		pthread_cond_init(&(rbuffers[i]->itemAvailable), NULL);
		pthread_cond_init(&(rbuffers[i]->spaceAvailable), NULL);
	}
    
    // mapper creation
    for(i = 0; i < N; i++){
        error = pthread_create(&(tidMapper[i]), NULL, &mapper, argv[i + 3]);
        if(error != 0){
            printf("failed to create mapper thread\n");
        }
    }
    
    // reducer creation
    for(i = 0; i < R; i++){
        error = pthread_create(&(tidReducer[i]), NULL, &reducer, NULL);
        if(error != 0){
            printf("failed to create reducer thread\n");
        }
    }
    
    // merger thread
    // allocate word array
	char **finalArray = (char**)malloc(250000 * sizeof(char*));
	int count = 0;
	// read from mapper and write to array for sorting
	for(i = 0; i < R; i++){
		// pthread_mutex_lock(&(buffers[i][j]->lock));
		// critical section entered
		// while(buffers[i][j]->count == 0 && buffers[i][j]->isDone == false){
                        // pthread_cond_wait(&(buffers[i][j]->itemAvailable), &(buffers[i][j]->lock));
                // }
        // if(buffers[i][j]->count == 0 && buffers[i][j]->isDone == true){
            // pthread_mutex_unlock(&(buffers[i][j]->lock));
        // }
		// else{
			
			while(rbuffers[i]->count > 0){
		    
				finalArray[count] = (char*)malloc(strlen(rbuffers[i]->buf[rbuffers[i]->readIndex]) + 1);
				//wordArray = (char**)realloc(wordArray, (count * sizeof(char*)));
			
				strcpy(finalArray[count], rbuffers[i]->buf[rbuffers[i]->readIndex]);
				count++;
			
				rbuffers[i]->readIndex = ((rbuffers[i]->readIndex + 1) % bufferSize);
				rbuffers[i]->count--;
				free(rbuffers[i]->buf[rbuffers[i]->readIndex]);
			}
			
			// pthread_cond_signal(&(buffers[i][j]->spaceAvailable));
			// pthread_mutex_unlock(&(buffers[i][j])->lock);
		//}		
	}
	
	qsort(finalArray, count, sizeof(char*), cmpstring);
    
	for(i = 0; i < count; i++){
		fprintf(fileFinal, "%s\n", finalArray[i]);
	}
    
	// condition destruction
	for(i = 0; i < N; i++){
		for(j = 0; j < R; j++){
			pthread_cond_destroy(&(buffers[i][j]->itemAvailable));
			pthread_cond_destroy(&(buffers[i][j]->spaceAvailable));
		}
	}
	for(i = 0; i < R; i++){
		pthread_cond_destroy(&(rbuffers[i]->itemAvailable));
		pthread_cond_destroy(&(rbuffers[i]->spaceAvailable));
	}
	
	// mutex destruction
	for(i = 0; i < N; i++){
		for(j = 0; j < R; j++){
			pthread_mutex_destroy(&(buffers[i][j]->lock));
		}
	}
	for(i = 0; i < R; i++){
		pthread_mutex_destroy(&(rbuffers[i]->lock));
	}
	
	for(i = 0; i < R; i++){
		free(finalArray[i]);
	}
	free(finalArray);
	for(i = 0; i < N; i++){
		for(j = 0; j < R; j++){
			free(buffers[i][j]);
		}
        free(buffers[i]);
    }
    free(buffers);
	for(i = 0; i < R; i++){
		free(rbuffers[i]);
	}
	free(rbuffers);
    free(tidMapper);
    free(tidReducer);
    
    fclose(fileFinal);

    exit(EXIT_SUCCESS);
}