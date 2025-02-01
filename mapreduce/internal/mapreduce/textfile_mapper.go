package mapreduce

type textFileMapper[K1, K2 comparable, V any] struct {
	mapper MapImplementation[K1, K2, V]
}

// NewTextFileMapper return a file mapper to a emmitter of pairs
func newTextFileMapper[K1, K2 comparable, V any](mapper MapImplementation[K1, K2, V]) *textFileMapper[K1, K2, V] {
	return &textFileMapper[K1, K2, V]{
		mapper: mapper,
	}
}

func (t *textFileMapper[K1, K2, V]) Map(in Iterator[string], out Emitter[Pair[K2, V]]) error {
	for in.Next() {
		line := in.Value()
		records, err := t.mapper.LineToRecords(line)
		if err != nil {
			return err
		}
		for _, v := range records {
			p, err := t.mapper.Map(v)
			if err != nil {
				return err
			}
			err = out.Emit(p)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// MapTextFile is the entry point you must call
// just provide filenames and split and map functions
func MapTextFile[K1, K2 comparable, V any](fnIn, fnOut string,
	mapper MapImplementation[K1, K2, V],
) error {
	fileMapper := newTextFileMapper(mapper)
	in, err := NewTextFileIterator(fnIn)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := newTextFileEmitterForMapper[K2, V](fnOut)
	if err != nil {
		return err
	}
	defer out.Close()
	return fileMapper.Map(in, out)
}
