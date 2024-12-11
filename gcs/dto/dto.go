package dto

type Counts struct {
	Less       int
	Equal      int
	More1To4   int
	More5To10  int
	MoreThan10 int
}

type NumFiles struct {
	Num int
	Err error
}
