#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/** AUTHORS
++   Suleyman Yasir Kula / 21200823
++   Tan Kucukoglu / 21201893
**/

// Inspired from: http://www.tavi.co.uk/phobos/fat.html
// Inspired from: http://en.wikipedia.org/wiki/Design_of_the_FAT_file_system
// Inspired from: http://wiki.osdev.org/FAT
// Inspired from: https://piazza.com/class/i5n24h3vyc063j

// 32-byte information for the most recently read file
unsigned char fileInfo[32];

// Block size in bytes
unsigned long blockSize;

// Number of reserved blocks before FAT blocks starts
unsigned long numberOfReservedBlocks;

// Retrieve a little-endian numeric value from a sequence of bytes
unsigned long getNumericValue( unsigned char* in, int offset, int length )
{
	unsigned long result = 0;
	int i;

	// First bytes are the least significant ones
	// Shift new bytes to the left so that previous (least significant) bytes remain on the right side
	for( i = length - 1; i >= 0; i-- )
	{
		result = result | ( in[offset+i] << ( i * 8 ) );
	}

	return result;
}

// Retrieve the starting block of a file from its directory entry
unsigned long getFirstBlock( unsigned char* in )
{
	return ( getNumericValue( in, 20, 2 ) << 16 ) | getNumericValue( in, 26, 2 );
}

// C version of Java's substring
char* substring( unsigned char* in, int from, int length )
{
	char* result = (char*) malloc(length+1);
	memcpy( result, &in[from], length );
	result[length] = '\0';
	
	return result;
}

// Read next 32-byte directory information from file descriptor 
// and return next file's name
// function returns NULL to specify that end of entries is reached
// if function returns "", there may be more entries after that file
char* getNextDirectory( int fileDescriptor )
{
	// Read next 32 bytes
	if( read( fileDescriptor, fileInfo, 32 ) < 0 )
	{
		printf( "ERROR: Could not read directory information!\n" );
		return NULL;
	}

	// If the attribute field of the file is invalid, return NULL (stop case)
	unsigned int attribute = getNumericValue( fileInfo, 11, 1 );
	if( attribute <= 0 )
	{
		return NULL;
	}
	
	// If file is deleted (fileInfo[0] = 0xE5), return ""
	// (listRootFolder will continue iterating)
	if( fileInfo[0] == 0xE5 )
	{
		return "";
	}
	
	// Retrieve filename from the fileInfo
	char *filename = substring( fileInfo, 0, 8 );
	
	if( attribute == 0x0F )
	{
		// It is a longfile
		free( filename );
		filename = NULL;
		int length = 0;
		int i;
		
		// Get the next longfile directory entry until standard directory entry is reached
		do
		{
			filename = (char*) realloc( filename, length + 14 );
			
			// Shift current characters (least significant chars) to right
			// so that next characters take place on the left hand side
			for( i = length; i >= 0; i-- )
			{
				filename[i+13]=filename[i];
			}
			
			filename[0] = fileInfo[1];
			filename[1] = fileInfo[3];
			filename[2] = fileInfo[5];
			filename[3] = fileInfo[7];
			filename[4] = fileInfo[9];
			filename[5] = fileInfo[14];
			filename[6] = fileInfo[16];
			filename[7] = fileInfo[18];
			filename[8] = fileInfo[20];
			filename[9] = fileInfo[22];
			filename[10] = fileInfo[24];
			filename[11] = fileInfo[28];
			filename[12] = fileInfo[30];
			
			length = strlen( filename );
			
			// Read next directory (whether or not a longfile is not known yet)
			if( read( fileDescriptor, fileInfo, 32 ) < 0 )
			{
				free( filename );
				printf( "ERROR: Could not read directory information for longfile\n" );
				return NULL;
			}
			attribute = getNumericValue( fileInfo, 11, 1 );
		} while( attribute == 0x0F );
		
		return filename;
	}
	else
	{
		// It is not a longfile
		// Fetch the extension of the file
		char *extension = substring( fileInfo, 8, 3 );
		
		// Combine the filename and extension together
		char *result = (char*) malloc( strlen( filename ) + strlen( extension ) + 1 );
		sprintf( result, "%s.%s", filename, extension );
		
		free( extension );
		
		return result;
	}
}

// List the files and folders inside the root directory
void listRootFolder( int fileDescriptor )
{
	// Fetch the first file from the root directory
	char *next = getNextDirectory( fileDescriptor );
	
	// While end of root directory is not reached
	while( next != NULL )
	{
		// If it is a valid file
		if( strlen( next ) > 0 )
		{
			// Print its name
			printf( "%s\n", next );
			free( next );
		}
		
		// Fetch the next file
		next = getNextDirectory( fileDescriptor );
	}
}

// Prints the blocks that a file consumes
int printBlocksForFile( int fileDescriptor, char* file )
{
	// Fetch the first file from the root directory
	char *next = getNextDirectory( fileDescriptor );
	
	// While end of root directory is not reached
	while( next != NULL )
	{
		// If it is a valid file
		if( strlen( next ) > 0 )
		{
			// If filenames match
			if( strcmp( next, file ) == 0 )
			{
				// If file's size is not 0
				unsigned long filesize = getNumericValue( fileInfo, 28, 4 );
				if( filesize > 0 )
				{
					// Fetch the start block of the file
					unsigned long startBlock = getFirstBlock( fileInfo );
					
					// While there are more blocks for this file
					while( startBlock < 0x0FFFFFF8 )
					{
						// Print the block number on console
						printf( "%lu\n", startBlock );
						
						// Find which FAT block to search for the next block of the file
						// (entry size = 4 bytes)
						unsigned long block = 4 * startBlock / blockSize;
						
						// Find the offset (in bytes)
						unsigned long offset = 4 * startBlock % blockSize;
					
						// Move the cursor to the beginning of the specific FAT entry
						if( lseek( fileDescriptor, ( block + numberOfReservedBlocks ) * blockSize + offset, SEEK_SET ) < 0 )
						{
							printf( "ERROR: Could not change the location of pointer to read FAT table!\n" );
							return -1;
						}
						
						// Fetch next block from corresponding FAT table entry
						unsigned char newLocation[4];
						if( read( fileDescriptor, newLocation, 4 ) < 0 )
						{
							printf( "ERROR: Could not read from FAT Table!\n" );
							return -1;
						}
						
						startBlock = getNumericValue( newLocation, 0, 4 );
					}
				}
				
				free( next );
				
				// Printed the blocks, return to main
				return 0;
			}
			
			// Filenames do not match, continue
			free( next );
		}
		
		// This was not a valid file (maybe a deleted file), get the next file
		next = getNextDirectory( fileDescriptor );
	}
	
	// Filename does not exist, stop
	return -1;
}
 
int main( int argc, char **argv )
{
	// File system to read
	int fileDescriptor;

	// Check if arguments are entered correctly
	if( argc > 3 )
	{
		printf( "ERROR: Program can take 2 argument at most!\n" );
		return -1;
	}

	if( argc == 1 )
	{
		printf( "ERROR: You need to pass the location of file system as parameter!\n" );
		return -1;             
	}

	// Open the file system
	fileDescriptor = open( argv[1], O_RDONLY );
	
	// Can not open the file system
	// Give an error
	if( fileDescriptor < 0 )
	{
		printf( "ERROR: Could not open %s!\n", argv[1] );
		return -1;
	}
	
	// Read the boot sector to get information about the file system
	unsigned char bootsector[100];
	if( read( fileDescriptor, bootsector, 100 ) < 0 )
	{
		printf( "ERROR: Could not read the boot sector!\n" );
		return -1;
	}
	
	// Fetch important values from the boot sector
	blockSize = getNumericValue( bootsector, 11, 2 );
	numberOfReservedBlocks = getNumericValue( bootsector, 14, 2 );
	unsigned long numberOfFATs = getNumericValue( bootsector, 16, 1 );
	unsigned long FATSize = getNumericValue( bootsector, 36, 4 );
	unsigned long rootDirectoryStartBlock = numberOfFATs * FATSize + numberOfReservedBlocks;

	// Go to the beginning of the root directory
	if( lseek( fileDescriptor, rootDirectoryStartBlock * blockSize, SEEK_SET ) < 0 )
	{
		printf( "ERROR: Could not change the location of pointer to read the root directory!\n" );
		return -1;
	}
	
	if( argc == 2 )
	{
		// No specific file is entered as parameter
		// Just print the contents of the root folder
		listRootFolder( fileDescriptor );
		printf( "\n" );
	}
	else
	{
		// Print the blocks that the specified file uses
		printBlocksForFile( fileDescriptor, argv[2] );
	}

	/*
	// Printing important information about the disk
	// (for test purposes)
	printf( "Number of reserved blocks: %lu\n", numberOfReservedBlocks );
	printf( "Bytes per logical sector: %lu\n", blockSize );
	printf( "Logical sectors per block: %lu\n", getNumericValue( bootsector, 13, 1 ) );
	printf( "Number of FATs: %lu\n", numberOfFATs );
	printf( "Logical sectors per FAT: %lu\n", FATSize );
	printf( "Root sector: %lu\n", getNumericValue( bootsector, 44, 4 ) );
	printf( "Root sector: %lu    -     %02X\n", rootDirectoryStartBlock, (unsigned int)(rootDirectoryStartBlock * blockSize) );
	*/
	
	return 0;
}