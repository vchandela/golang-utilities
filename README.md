# golang-utilities

- gcs/
    - fileBasedComparison(): compare no. of files in a folder across 2 buckets based on IDs in file.txt
    - bucketBasedComparison(): compare no. of files in a folder across 2 buckets based on IDs in 2nd bucket
- ocs/
    - fileBasedComparison(): compare no. of files in a folder across 2 buckets based on IDs in file.txt
    - bucketBasedComparison(): TODO

- ToDo:
    - modularize code for ocs similar to gcs
    - write common functions across ocs and gcs in a separate package

### Run
- `go run main.go` inside `gcs/` or `ocs/`
- `output.txt` will contain the IDs where no. of files in 1st bucket is greater than 2nd bucket.
    - Also, it will have a summary of the comparison at the bottom.