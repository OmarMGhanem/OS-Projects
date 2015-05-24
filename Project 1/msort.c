
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>

typedef struct aNode{
	int item;
	struct aNode *next;
} node;

int main(int argc, char **argv){

    pid_t pid;
    pid = fork();

	return EXIT_SUCCESS;
}
