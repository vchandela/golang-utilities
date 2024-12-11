package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"gcs_path/dto"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// countFilesInGCS checks no. of files at a specific prefix (path) in the GCS bucket.
func countFilesInGCS(ctx context.Context, client *storage.Client, bucketName, prefix string, ch chan dto.NumFiles, wg *sync.WaitGroup) {
	defer wg.Done()

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
			// error occurred
			ch <- dto.NumFiles{Num: 0, Err: err}
			return
		}
		count++
	}
	ch <- dto.NumFiles{Num: count, Err: nil}
}

func compareNumFilesAcrossBuckets(ctx context.Context, client *storage.Client, id, bucket, bucket2, rootPrefix, rootPrefix2 string, logger *log.Logger) (int, int, error) {
	prefix1 := fmt.Sprintf("%s%s", rootPrefix, id)
	prefix2 := fmt.Sprintf("%s%s", rootPrefix2, id)

	c1 := make(chan dto.NumFiles, 1)
	c2 := make(chan dto.NumFiles, 1)

	var wg sync.WaitGroup
	wg.Add(2)
	go countFilesInGCS(ctx, client, bucket, prefix1, c1, &wg)
	go countFilesInGCS(ctx, client, bucket2, prefix2, c2, &wg)

	// Close channels after workers complete
	go func() {
		wg.Wait()
		close(c1)
		close(c2)
	}()

	var numFiles, numFiles2 dto.NumFiles
	for i := 0; i < 2; i++ {
		select {
		case result := <-c1:
			if result.Err != nil {
				logger.Printf("Error checking prefix existence for ID '%s' in bucket '%s': %v", id, bucket, result.Err)
				return 0, 0, result.Err
			}
			numFiles = result
		case result := <-c2:
			if result.Err != nil {
				logger.Printf("Error checking prefix existence for ID '%s' in bucket '%s': %v", id, bucket2, result.Err)
				return 0, 0, result.Err
			}
			numFiles2 = result
		case <-time.After(5 * time.Minute): // Adjust timeout as needed
			logger.Printf("Timeout while waiting for data for ID '%s'", id)
			return 0, 0, fmt.Errorf("timeout while waiting for data for ID '%s'", id)
		}
	}
	return numFiles.Num, numFiles2.Num, nil
}

func fileBasedComparison(ctx context.Context, client *storage.Client, bucket, bucket2, rootPrefix, rootPrefix2 string, logger *log.Logger) {
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

		numFiles, numFiles2, err := compareNumFilesAcrossBuckets(ctx, client, id, bucket, bucket2, rootPrefix, rootPrefix2, logger)
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
			if len(pathArr) > 2 {
				id := pathArr[2]
				if id != "" {
					ids = append(ids, id)
				}
			}
		}
	}

	return ids, nil
}

func calculateCounts(ctx context.Context, client *storage.Client, bucket, bucket2, rootPrefix, rootPrefix2, id string, logger *log.Logger, cnt *dto.Counts, mutex *sync.Mutex, badIds *[]string) {
	numFiles, numFiles2, err := compareNumFilesAcrossBuckets(ctx, client, id, bucket, bucket2, rootPrefix, rootPrefix2, logger)
	if err != nil {
		return
	}

	mutex.Lock()
	defer mutex.Unlock() // Ensure safe access to shared state

	if numFiles < numFiles2 {
		cnt.Less++
	} else if numFiles == numFiles2 {
		cnt.Equal++
	} else if numFiles > numFiles2 && numFiles <= numFiles2+4 {
		cnt.More1To4++
	} else if numFiles >= numFiles2+5 && numFiles <= numFiles2+10 {
		cnt.More5To10++
	} else {
		logger.Printf("livestream '%s': bucket1 '%s': %d file(s): bucket2 '%s': %d file(s); diff: %d\n", id, bucket, numFiles, bucket2, numFiles2, numFiles-numFiles2)
		logger.Println("--------------------------------------")
		cnt.MoreThan10++
        *badIds = append(*badIds, id)
	}
}

func worker(ctx context.Context, client *storage.Client, bucket, bucket2, rootPrefix, rootPrefix2 string, logger *log.Logger, cnt *dto.Counts, mutex *sync.Mutex, jobs <-chan string, wg *sync.WaitGroup, badIds *[]string) {
    defer wg.Done()
	for id := range jobs {
		calculateCounts(ctx, client, bucket, bucket2, rootPrefix, rootPrefix2, id, logger, cnt, mutex, badIds)
	}
}

func bucketBasedComparison(ctx context.Context, client *storage.Client, bucket, bucket2, rootPrefix, rootPrefix2 string, logger *log.Logger) {
	ids, err := getUniqueIDsFromGCS(ctx, client, bucket2, rootPrefix2)
	if err != nil {
		logger.Fatalf("Failed to retrieve IDs from temp bucket: %v", err)
	}

	logger.Printf("Total IDs in temp bucket: %d\n", len(ids))

	jobs := make(chan string, len(ids))
	var mutex sync.Mutex
    var wg sync.WaitGroup
	cnt := dto.Counts{} //contains shared variables
    badIds := make([]string, 0)

    for _, id := range ids {
		jobs <- id
	}
	close(jobs)

	for w := 1; w <= 1024; w++ {
        wg.Add(1)
		go worker(ctx, client, bucket, bucket2, rootPrefix, rootPrefix2, logger, &cnt, &mutex, jobs, &wg, &badIds)
	}

    wg.Wait()

	logger.Printf("Total IDs in temp bucket: %d\n", len(ids))
	logger.Printf("Total IDs with less files in prod bucket than temp bucket: %d\n", cnt.Less)
	logger.Printf("Total IDs with same files in prod bucket and temp bucket: %d\n", cnt.Equal)
	logger.Printf("Total IDs with 1-4 more files in prod bucket than temp bucket: %d\n", cnt.More1To4)
	logger.Printf("Total IDs with 5-10 more files in prod bucket than temp bucket: %d\n", cnt.More5To10)
	logger.Printf("Total IDs with more than 10 files in prod bucket than temp bucket: %d\n", cnt.MoreThan10)
    // Add single quotes to each element
	for i, v := range badIds {
		badIds[i] = "'" + v + "'"
	}
    result := "(" + strings.Join(badIds, ", ") + ")"
    logger.Printf("Bad IDs: %s\n", result)
}

func main() {
	bucket := "livestream-recording-service-prod-bucket"
	rootPrefix := "CompositePreProcessing/v2/"
	bucket2 := "livestream-recording-service-prod-bucket-temp"
	rootPrefix2 := "CompositePreProcessing/v4/"

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

	// fileBasedComparison(ctx, client, bucket, bucket2, rootPrefix, rootPrefix2, logger)
	bucketBasedComparison(ctx, client, bucket, bucket2, rootPrefix, rootPrefix2, logger)
}
