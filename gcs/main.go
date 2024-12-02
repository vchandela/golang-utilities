package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// countFilesInGCS checks no. of files at a specific prefix (path) in the GCS bucket.
func countFilesInGCS(ctx context.Context, client *storage.Client, bucketName, prefix string) (int, error) {
	query := &storage.Query{Prefix: prefix}

	it := client.Bucket(bucketName).Objects(ctx, query)
	count := 0

	// Iterate over objects and count them
	for {
		_, err := it.Next()
		if err == iterator.Done {
			// No more objects to iterate
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to list objects: %w", err)
		}
		count++
	}

	return count, nil
}

func main() {
	bucketName := "livestream-recording-service-prod-bucket"
	bucketName2 := "livestream-recording-service-prod-bucket-temp"

	// Open the file containing IDs
	file, err := os.Open("file.txt")
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// single GCS client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}
	defer client.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		id := strings.TrimSpace(scanner.Text())
		if id == "" {
			continue
		}

		prefix := fmt.Sprintf("CompleteLivestreamRecording/%s/compositeRecording", id)
		numFiles, numFiles2 := 0, 0

		numFiles, err = countFilesInGCS(ctx, client, bucketName, prefix)
		if err != nil {
			log.Printf("Error checking prefix existence for ID '%s': %v", id, err)
			continue
		}

		numFiles2, err = countFilesInGCS(ctx, client, bucketName2, prefix)
		if err != nil {
			log.Printf("Error checking prefix existence for ID '%s': %v", id, err)
			continue
		}

		fmt.Printf("livestream '%s': bucket1 '%s': %d file(s): bucket2 '%s': %d file(s); diff: %d\n", id, bucketName, numFiles, bucketName2, numFiles2, numFiles-numFiles2)
		fmt.Println("--------------------------------------")
	}

	// Check for errors during file reading
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
}
