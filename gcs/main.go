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

func calculateCounts(ctx context.Context, client *storage.Client, bucket, bucket2, rootPrefix, rootPrefix2, id string, logger *log.Logger, cnt *dto.Counts, mutex *sync.Mutex, badIds map[string][]string) {
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
	} else if numFiles >= numFiles2+11 && numFiles <= numFiles2+20 {
		cnt.More11To20++
        badIds["11To20"] = append(badIds["11To20"], id)
	} else if numFiles >= numFiles2+21 && numFiles <= numFiles2+30 {
		cnt.More21To30++
        badIds["21To30"] = append(badIds["21To30"], id)
	} else if numFiles >= numFiles2+31 && numFiles <= numFiles2+40 {
		cnt.More31To40++
        badIds["31To40"] = append(badIds["31To40"], id)
	} else if numFiles >= numFiles2+41 && numFiles <= numFiles2+50 {
		cnt.More41To50++
        badIds["41To50"] = append(badIds["41To50"], id)
	} else if numFiles >= numFiles2+51 && numFiles <= numFiles2+60 {
		cnt.More51To60++
        badIds["51To60"] = append(badIds["51To60"], id)
	} else if numFiles >= numFiles2+61 && numFiles <= numFiles2+70 {
		cnt.More61To70++
        badIds["61To70"] = append(badIds["61To70"], id)
	} else if numFiles >= numFiles2+71 && numFiles <= numFiles2+80 {
		cnt.More71To80++
        badIds["71To80"] = append(badIds["71To80"], id)
	} else if numFiles >= numFiles2+81 && numFiles <= numFiles2+90 {
		cnt.More81To90++
        badIds["81To90"] = append(badIds["81To90"], id)
	} else if numFiles >= numFiles2+91 && numFiles <= numFiles2+100 {
		cnt.More91To100++
        badIds["91To100"] = append(badIds["91To100"], id)
	} else if numFiles >= numFiles2+101 && numFiles <= numFiles2+150 {
		cnt.More101To150++
        badIds["101To150"] = append(badIds["101To150"], id)
	} else if numFiles >= numFiles2+151 && numFiles <= numFiles2+200 {
		cnt.More151To200++
        badIds["151To200"] = append(badIds["151To200"], id)
	} else if numFiles >= numFiles2+201 && numFiles <= numFiles2+250 {
		cnt.More201To250++
        badIds["201To250"] = append(badIds["201To250"], id)
	} else if numFiles >= numFiles2+251 && numFiles <= numFiles2+300 {
		cnt.More251To300++
        badIds["251To300"] = append(badIds["251To300"], id)
	} else if numFiles >= numFiles2+301 && numFiles <= numFiles2+350 {
		cnt.More301To350++
        badIds["301To350"] = append(badIds["301To350"], id)
	} else if numFiles >= numFiles2+351 && numFiles <= numFiles2+400 {
		cnt.More351To400++
        badIds["351To400"] = append(badIds["351To400"], id)
	} else if numFiles >= numFiles2+401 && numFiles <= numFiles2+450 {
		cnt.More401To450++
        badIds["401To450"] = append(badIds["401To450"], id)
	} else if numFiles >= numFiles2+451 && numFiles <= numFiles2+500 {
		cnt.More451To500++
        badIds["451To500"] = append(badIds["451To500"], id)
	} else {
		cnt.MoreThan500++
        badIds["MoreThan500"] = append(badIds["MoreThan500"], id)
	}
}

func worker(ctx context.Context, client *storage.Client, bucket, bucket2, rootPrefix, rootPrefix2 string, logger *log.Logger, cnt *dto.Counts, mutex *sync.Mutex, jobs <-chan string, wg *sync.WaitGroup, badIds map[string][]string) {
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

	jobs := make(chan string, len(ids))
	var mutex sync.Mutex
	var wg sync.WaitGroup
	cnt := dto.Counts{} //contains shared variables
	badIds := make(map[string][]string)
	badIds["11To20"] = []string{}
	badIds["21To30"] = []string{}
	badIds["31To40"] = []string{}
	badIds["41To50"] = []string{}
	badIds["51To60"] = []string{}
	badIds["61To70"] = []string{}
	badIds["71To80"] = []string{}
	badIds["81To90"] = []string{}
	badIds["91To100"] = []string{}
	badIds["101To150"] = []string{}
	badIds["151To200"] = []string{}
	badIds["201To250"] = []string{}
	badIds["251To300"] = []string{}
	badIds["301To350"] = []string{}
	badIds["351To400"] = []string{}
	badIds["401To450"] = []string{}
	badIds["451To500"] = []string{}
	badIds["MoreThan500"] = []string{}

	for _, id := range ids {
		jobs <- id
	}
	close(jobs)

	for w := 1; w <= 1024; w++ {
		wg.Add(1)
		go worker(ctx, client, bucket, bucket2, rootPrefix, rootPrefix2, logger, &cnt, &mutex, jobs, &wg, badIds)
	}

	wg.Wait()

	logger.Printf("Total IDs in temp bucket: %d\n", len(ids))
	logger.Printf("Total IDs with less files in prod bucket than temp bucket: %d\n", cnt.Less)
	logger.Printf("Total IDs with same files in prod bucket and temp bucket: %d\n", cnt.Equal)
	logger.Printf("Total IDs with 1-4 more files in prod bucket than temp bucket: %d\n", cnt.More1To4)
	logger.Printf("Total IDs with 5-10 more files in prod bucket than temp bucket: %d\n", cnt.More5To10)
	logger.Printf("Total IDs with 11-20 more files in prod bucket than temp bucket: %d\n", cnt.More11To20)
	logger.Printf("Total IDs with 21-30 more files in prod bucket than temp bucket: %d\n", cnt.More21To30)
	logger.Printf("Total IDs with 31-40 more files in prod bucket than temp bucket: %d\n", cnt.More31To40)
	logger.Printf("Total IDs with 41-50 more files in prod bucket than temp bucket: %d\n", cnt.More41To50)
	logger.Printf("Total IDs with 51-60 more files in prod bucket than temp bucket: %d\n", cnt.More51To60)
	logger.Printf("Total IDs with 61-70 more files in prod bucket than temp bucket: %d\n", cnt.More61To70)
	logger.Printf("Total IDs with 71-80 more files in prod bucket than temp bucket: %d\n", cnt.More71To80)
	logger.Printf("Total IDs with 81-90 more files in prod bucket than temp bucket: %d\n", cnt.More81To90)
	logger.Printf("Total IDs with 91-100 more files in prod bucket than temp bucket: %d\n", cnt.More91To100)
	logger.Printf("Total IDs with 101-150 more files in prod bucket than temp bucket: %d\n", cnt.More101To150)
	logger.Printf("Total IDs with 151-200 more files in prod bucket than temp bucket: %d\n", cnt.More151To200)
	logger.Printf("Total IDs with 201-250 more files in prod bucket than temp bucket: %d\n", cnt.More201To250)
	logger.Printf("Total IDs with 251-300 more files in prod bucket than temp bucket: %d\n", cnt.More251To300)
	logger.Printf("Total IDs with 301-350 more files in prod bucket than temp bucket: %d\n", cnt.More301To350)
	logger.Printf("Total IDs with 351-400 more files in prod bucket than temp bucket: %d\n", cnt.More351To400)
	logger.Printf("Total IDs with 401-450 more files in prod bucket than temp bucket: %d\n", cnt.More401To450)
	logger.Printf("Total IDs with 451-500 more files in prod bucket than temp bucket: %d\n", cnt.More451To500)
	logger.Printf("Total IDs with more than 500 files in prod bucket than temp bucket: %d\n", cnt.MoreThan500)

    for key, slice := range badIds {
		// Add single quotes to each element in the slice
		for i, v := range slice {
			slice[i] = "'" + v + "'"
		}

		// Join the elements with a comma and wrap in brackets
		result := "(" + strings.Join(slice, ", ") + ")"

		// Print the key and the formatted slice
		logger.Printf("%s: %s\n", key, result)
	}
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
