package dto

type Counts struct {
	Less         int
	Equal        int
	More1To4     int
	More5To10    int
	More11To20   int
	More21To30   int
	More31To40   int
	More41To50   int
	More51To60   int
	More61To70   int
	More71To80   int
	More81To90   int
	More91To100  int
	MoreThan100  int
	More101To150 int
	More151To200 int
	More201To250 int
	More251To300 int
	More301To350 int
	More351To400 int
	More401To450 int
	More451To500 int
	MoreThan500  int
}

type NumFiles struct {
	Num int
	Err error
}
