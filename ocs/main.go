package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// countFilesInOCS checks if a specific prefix (path) exists in the OCS bucket
// and counts the number of objects that match the prefix.
func countFilesInOCS(ctx context.Context, client objectstorage.ObjectStorageClient, namespace, bucketName, prefix string) (int, error) {
	// ListObjects request with the prefix
	request := objectstorage.ListObjectsRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketName,
		Prefix:        &prefix,
	}

	// Response to list objects
	response, err := client.ListObjects(ctx, request)
	if err != nil {
		return 0, fmt.Errorf("failed to list objects: %w", err)
	}

	// Return the count of objects in the response
	return len(response.Objects), nil
}

func main() {
	bucketName := "livestream-recording-service-stage-bucket"
	namespace := "bmejw7lmibdo"
	bucketName2 := "livestream-recording-service-stage-bucket"
	namespace2 := "bmejw7lmibdo"

	// Open the file containing IDs
	file, err := os.Open("file.txt")
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	ctx := context.Background()
	provider := common.DefaultConfigProvider() // Reads configuration from ~/.oci/config
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		log.Fatalf("Failed to create object storage client: %v", err)
	}

	// Read file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		id := strings.TrimSpace(scanner.Text())
		if id == "" {
			continue
		}

		// Build the prefix using the ID
		prefix := fmt.Sprintf("CompleteLivestreamRecording/%s/compositeRecording", id)
		numFiles, numFiles2 := 0, 0

		numFiles, err = countFilesInOCS(ctx, client, namespace, bucketName, prefix)
		if err != nil {
			log.Printf("Error checking prefix existence or counting files for ID '%s': %v", id, err)
			continue
		}

		numFiles2, err = countFilesInOCS(ctx, client, namespace2, bucketName2, prefix)
		if err != nil {
			log.Printf("Error checking prefix existence or counting files for ID '%s': %v", id, err)
			continue
		}

		fmt.Printf("livestream '%s': bucket1 '%s': %d file(s): bucket2 '%s': %d file(s); diff: %d\n", id, bucketName, numFiles, bucketName2, numFiles2, numFiles-numFiles2)

		fmt.Println("--------------------------------------------------")
	}

	// Check for errors during file reading
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
}
