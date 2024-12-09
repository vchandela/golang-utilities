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

func compareNumFilesAcrossBuckets(ctx context.Context, client *storage.Client, id, bucket, bucket2, rootPrefix string, logger *log.Logger) (int, int, error) {
	prefix := fmt.Sprintf("%s%s", rootPrefix, id)
	numFiles, numFiles2 := 0, 0

	numFiles, err := countFilesInGCS(ctx, client, bucket, prefix)
	if err != nil {
		logger.Printf("Error checking prefix existence for ID '%s': %v", id, err)
		return 0, 0, err
	}

	numFiles2, err = countFilesInGCS(ctx, client, bucket2, prefix)
	if err != nil {
		logger.Printf("Error checking prefix existence for ID '%s': %v", id, err)
		return 0, 0, err
	}
	return numFiles, numFiles2, nil
}

func fileBasedComparison(ctx context.Context, client *storage.Client, bucket, bucket2, rootPrefix string, logger *log.Logger) {
	// Open the file containing IDs
	file, err := os.Open("file.txt")
	if err != nil {
		logger.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	cntLess, cntEq, cntMore := 0, 0, 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		id := strings.TrimSpace(scanner.Text())
		if id == "" {
			continue
		}

		numFiles, numFiles2, err := compareNumFilesAcrossBuckets(ctx, client, id, bucket, bucket2, rootPrefix, logger)
		if err != nil {
			continue
		}

		if numFiles < numFiles2 {
			cntLess++
		} else if numFiles == numFiles2 {
			cntEq++
		} else {
			logger.Printf("livestream '%s': bucket1 '%s': %d file(s): bucket2 '%s': %d file(s); diff: %d\n", id, bucket, numFiles, bucket2, numFiles2, numFiles-numFiles2)
			logger.Println("--------------------------------------")
			cntMore++
		}
	}
	// Check for errors during file reading
	if err := scanner.Err(); err != nil {
		logger.Fatalf("Error reading file: %v", err)
	}
	logger.Printf("Total IDs in temp bucket: %d\n", cntLess+cntEq+cntMore)
	logger.Printf("Total IDs with less files in prod bucket than temp bucket: %d\n", cntLess)
	logger.Printf("Total IDs with same files in prod bucket and temp bucket: %d\n", cntEq)
	logger.Printf("Total IDs with more files in prod bucket than temp bucket: %d\n", cntMore)
}

func getUniqueIDsFromGCS(ctx context.Context, client *storage.Client, bucketName, rootPrefix string) ([]string, error) {
	query := &storage.Query{Prefix: rootPrefix, Delimiter: "/"}
	it := client.Bucket(bucketName).Objects(ctx, query)
	var ids []string

	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		if objAttrs.Prefix != "" {
			// Extract ID (e.g fc8a9074-87d7-4c08-a0cb-ed4c00e0e91d) from the prefix (e.g CompositePreProcessing/v2/fc8a9074-87d7-4c08-a0cb-ed4c00e0e91d/)
			pathArr := strings.Split(objAttrs.Prefix, "/")
			if len(pathArr) > 2 && pathArr[2] != "" {
				ids = append(ids, pathArr[2])
			}
		}
	}

	return ids, nil
}

func bucketBasedComparison(ctx context.Context, client *storage.Client, bucket, bucket2, rootPrefix string, logger *log.Logger) {
	ids, err := getUniqueIDsFromGCS(ctx, client, bucket2, rootPrefix)
	if err != nil {
		logger.Fatalf("Failed to retrieve IDs from temp bucket: %v", err)
	}

	cntLess, cntEq, cntMore := 0, 0, 0
	for _, id := range ids {
		numFiles, numFiles2, err := compareNumFilesAcrossBuckets(ctx, client, id, bucket, bucket2, rootPrefix, logger)
		if err != nil {
			continue
		}
		if numFiles < numFiles2 {
			cntLess++
		} else if numFiles == numFiles2 {
			cntEq++
		} else {
			logger.Printf("livestream '%s': bucket1 '%s': %d file(s): bucket2 '%s': %d file(s); diff: %d\n", id, bucket, numFiles, bucket2, numFiles2, numFiles-numFiles2)
			logger.Println("--------------------------------------")
			cntMore++
		}
	}
	logger.Printf("Total IDs in temp bucket: %d\n", len(ids))
	logger.Printf("Total IDs with less files in prod bucket than temp bucket: %d\n", cntLess)
	logger.Printf("Total IDs with same files in prod bucket and temp bucket: %d\n", cntEq)
	logger.Printf("Total IDs with more files in prod bucket than temp bucket: %d\n", cntMore)
}

func main() {
	bucket := "livestream-recording-service-prod-bucket"
	bucket2 := "livestream-recording-service-prod-bucket-temp"
	rootPrefix := "CompositePreProcessing/v2/"

	// single GCS client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}
	defer client.Close()

	// Open or create the output file
	outputFile, err := os.Create("output.txt")
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outputFile.Close()

	// Redirect output to the file
	logger := log.New(outputFile, "", 0)

	// fileBasedComparison(ctx, client, bucket, bucket2, rootPrefix, logger)
	bucketBasedComparison(ctx, client, bucket, bucket2, rootPrefix, logger)
}
