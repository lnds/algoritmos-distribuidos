package mapreduce

import (
	"bufio"
	"log"
	"os"
)

type textFileIterator struct {
	file    *os.File
	scanner *bufio.Scanner
}

func (t *textFileIterator) Close() error {
	return t.file.Close()
}

func (t *textFileIterator) Next() bool {
	return t.scanner.Scan()
}

func (t *textFileIterator) Value() string {
	return t.scanner.Text()
}

func (t *textFileIterator) Error() error {
	return t.scanner.Err()
}

func NewTextFileIterator(fname string) (Iterator[string], error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	return &textFileIterator{
		file:    file,
		scanner: scanner,
	}, nil
}

func OpenTextFileLineIterators(
	fnames []string,
) (iterators []Iterator[string], err error) {
	for _, name := range fnames {
		iter, err := NewTextFileIterator(name)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, iter)
	}
	return
}

func CloseTextFileLineIterators(iters []Iterator[string], names []string) error {
	for i := 0; i < len(iters); i++ {
		err := iters[i].Close()
		if err != nil {
			log.Printf("error closing %s: %v\n", names[i], err)
			return err
		}
	}
	return nil
}
