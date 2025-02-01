package mapreduce

import (
	"fmt"
	"log"
)

func SplitTextFile(fname string, prefix string, parts int) ([]string, error) {
	emitters, names, err := OpenTextFileLineEmitters(prefix, parts)
	if err != nil {
		return nil, err
	}
	iter, err := NewTextFileIterator(fname)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = iter.Close()
		_ = CloseTextFileLineEmitters(emitters, names)
	}()
	i := 0
	for iter.Next() {
		err := emitters[i].Emit(iter.Value())
		if err != nil {
			log.Printf("error writing to %s: %v\n", names[i], err)
			return nil, err
		}
		i = (i + 1) % parts
	}
	return names, nil
}

func OpenTextFileLineEmitters(
	prefix string,
	parts int,
) (emitters []Emitter[string], names []string, err error) {
	for i := 0; i < parts; i++ {
		name := fmt.Sprintf("%s-%d.txt", prefix, i)
		emitter, err := NewTextFileLineEmitter(name)
		if err != nil {
			return nil, nil, err
		}
		names = append(names, name)
		emitters = append(emitters, emitter)
	}
	return
}

func CloseTextFileLineEmitters(emitters []Emitter[string], names []string) error {
	for i := 0; i < len(emitters); i++ {
		err := emitters[i].Close()
		if err != nil {
			log.Printf("error closing %s: %v\n", names[i], err)
			return err
		}
	}
	return nil
}
