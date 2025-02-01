package mapreduce

import (
	"bufio"
	"fmt"
	"os"
)

type textFileEmitterForMapper[K comparable, V any] struct {
	file   *os.File
	writer *bufio.Writer
}

// NewTextFileEmitterForMapper create en emiter for text files
func newTextFileEmitterForMapper[K comparable, V any](fname string) (Emitter[Pair[K, V]], error) {
	file, err := os.Create(fname)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)
	return &textFileEmitterForMapper[K, V]{
		file:   file,
		writer: writer,
	}, nil
}

func (t *textFileEmitterForMapper[K, V]) Close() error {
	t.writer.Flush()
	return t.file.Close()
}

func (t *textFileEmitterForMapper[K, V]) Emit(value Pair[K, V]) error {
	_, err := t.writer.WriteString(fmt.Sprintf("%s\n", value))
	return err
}

func (p Pair[K, V]) String() string {
	return fmt.Sprintf(`%v,%v`, p.Key, p.Value)
}

type textFileLineEmitter struct {
	file   *os.File
	writer *bufio.Writer
}

func NewTextFileLineEmitter(fname string) (Emitter[string], error) {
	file, err := os.Create(fname)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)
	return &textFileLineEmitter{
		file:   file,
		writer: writer,
	}, nil
}

func (t *textFileLineEmitter) Close() error {
	t.writer.Flush()
	return t.file.Close()
}

func (t *textFileLineEmitter) Emit(value string) error {
	_, err := t.writer.WriteString(fmt.Sprintf("%s\n", value))
	return err
}

type textFileEmitterForReducer[K comparable, V any] struct {
	file   *os.File
	writer *bufio.Writer
}

func (t *textFileEmitterForReducer[K, V]) Emit(v []Pair[K, V]) error {
	if len(v) == 1 {
		_, err := t.writer.WriteString(fmt.Sprintf("%s\n", v[0]))
		return err
	}
	_, err := t.writer.WriteString(fmt.Sprintf("%s\n", v))
	return err
}

func (t *textFileEmitterForReducer[K, V]) Close() error {
	t.writer.Flush()
	return t.file.Close()
}

func newTextFileEmitterForReducer[K comparable, V any](fname string) (Emitter[[]Pair[K, V]], error) {
	file, err := os.Create(fname)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(file)
	return &textFileEmitterForReducer[K, V]{
		file:   file,
		writer: writer,
	}, nil
}
