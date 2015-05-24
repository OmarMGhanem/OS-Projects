#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define MAX_INPUT 100
#define MAX_LENGTH 256
#define MAX_TEMP 50

// global variables
int tempCount;
int inputCount;

FILE* **allFiles;

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
    
    int j = 0;
    
    char *fileName;
    fileName = (char*)arg;
    
    char word[MAX_LENGTH];
    
    pthread_t id = pthread_self();
    
    FILE *inputFile;
    
    // correct thread selection
    while(pthread_equal(id, tidMapper[j]) == 0){
        j++; 
    }
    
    // read input files then partition words into intermediates
    // according to their hash results
    inputFile = fopen(fileName, "r");
    while(fscanf(inputFile, "%255s", word) != EOF){
        int i;
        i = hash(word) % tempCount;
        fprintf(allFiles[j][i], "%s\n", word);
    }
    fclose(inputFile);
}

// reducer thread function
void *reducer(void *arg){
    
    int i = 0, j = 0, k = 0; 
    int count = 0;
    int sameCount = 0;
    char word[MAX_LENGTH];
    
    FILE* reducerTemp;
    char *reducerTempName = (char*)malloc(1000 * sizeof(char));
    
    pthread_t id = pthread_self();
    
    // correct thread selection
    while(pthread_equal(id, tidReducer[j]) == 0){
       j++;
    }
    
    sprintf(reducerTempName, "temp%d", j);
    reducerTemp = fopen(reducerTempName, "w+");
    
    // rewind all files since they were at EOF from mapper threads
    for(i = 0; i < inputCount; i++){
        rewind(allFiles[i][j]);
    }
    
    // find word array size
    for(i = 0; i < inputCount; i++){
        while(fscanf(allFiles[i][j], "%255s", word) != EOF){
            count++;
        }
    }
    
    // allocate word array
    char **wordArray = (char**)malloc(count * sizeof(char*));
    
    // rewind the files back
    for(i = 0; i < inputCount; i++){
        rewind(allFiles[i][j]);
    }
    
    // put words in word array
    for(i = 0; i < inputCount; i++){
        while(fscanf(allFiles[i][j], "%255s", word) != EOF){
            wordArray[k] = (char*)malloc(strlen(word) + 1);
            strcpy(wordArray[k], word);
            k++;
        }
    }
    
    // sort words
    qsort(wordArray, count, sizeof(char*), cmpstring);
    
    // count unique words and write in tempx file
    for(i = 1; i < count; i++){
        if(wordArray[i - 1] == wordArray[i]){
            sameCount++;
        }
        else{
            sameCount++; // need + 1 because counting starts from the bottom
            fprintf(reducerTemp, "%s %d\n", wordArray[i - 1], sameCount);
            sameCount = 0;
        }
        
    }
    
    fclose(reducerTemp);
    
    for(i = 0; i < count; i++){
        free(wordArray[i]);
    }
    free(wordArray);
    free(reducerTempName);
}

int main(int argc, char **argv){
    
    // main thread, also merger thread
    
    inputCount = atoi(argv[1]); // N
    tempCount = atoi(argv[2]); // R
    int error = 0, i, j;
    
    char *tempName = (char*)malloc(1000 * sizeof(char));
    //char *mergingName = (char*)malloc(1000 * sizeof(char));
    
    //char **wordArray = (char**)malloc(tempCount * sizeof(char*));
    
    tidMapper = (pthread_t*)malloc(inputCount * sizeof(pthread_t));
    tidReducer = (pthread_t*)malloc(tempCount * sizeof(pthread_t));
    
    // dynamic mem alloc of 2d file array
    allFiles = (FILE* **)malloc(inputCount * sizeof(FILE* *));
    for(i = 0; i < inputCount; i++){
        allFiles[i] = (FILE* *)malloc(tempCount * sizeof(FILE*));
    }
    
    FILE* fileFinal = fopen(argv[argc - 1], "w+");
    
    //FILE* *mergingFiles = (FILE* *)malloc(tempCount * sizeof(FILE*));
    
    //for(i = 0; i < tempCount; i++){
    //    sprintf(mergingName, "temp%d", i);
    //    mergingFiles[i] = fopen(mergingName, "w+");
    //}
    
    // routine checks
    if(tempCount > MAX_TEMP){
        perror("Too many temporary intermediate files!\n");
        exit(EXIT_FAILURE);
    }
    if(inputCount > MAX_INPUT){
        perror("Too many input files!\n");
        exit(EXIT_FAILURE);
    }
    
    // open intermediate files
    for(j = 0; j < inputCount; j++){
        for(i = 0; i < tempCount; i++){
            sprintf(tempName, "temp%d-%d", j, i);
            allFiles[j][i] = fopen(tempName, "w+");
        }
    }
    
    // mapper creation
    for(i = 0; i < inputCount; i++){
        error = pthread_create(&(tidMapper[i]), NULL, &mapper, argv[i + 3]);
        if(error != 0){
            printf("failed to create mapper thread\n");
        }
    }
    
    // mapper termination
    for(i = 0; i < inputCount; i++){
        pthread_join(tidMapper[i], NULL);
    }
    
    // reducer creation
    for(i = 0; i < tempCount; i++){
        error = pthread_create(&(tidReducer[i]), NULL, &reducer, NULL);
        if(error != 0){
            printf("failed to create reducer thread\n");
        }
    }
    
    // reducer termination
    for(i = 0; i < tempCount; i++){
        pthread_join(tidReducer[i], NULL);
    }
    
    // merging tempx files and sorting fileFinal
    
    // close intermediate files
    for(j = 0; j < inputCount; j++){
        for(i = 0; i < tempCount; i++){
            fclose(allFiles[j][i]);
        }
    }
    //for(i = 0; i < tempCount; i++){
    //    fclose(mergingFiles[i]);
    //}
    fclose(fileFinal);
    
    // deallocate memory
    free(tempName);
    //free(mergingName);
    //free(mergingFiles);
    //for(i = 0; i < tempCount; i++){
    //    free(wordArray[i]);
    //}
    //free(wordArray);
    free(tidMapper);
    free(tidReducer);
    for(i = 0; i < inputCount; i++){
        free(allFiles[i]);
    }
    free(allFiles);

    exit(EXIT_SUCCESS);
}