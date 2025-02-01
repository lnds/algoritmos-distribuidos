package mapreduce

type textFileReducer[K1, K2 comparable, V1, V2 any] struct {
	reducer ReduceImplementation[K1, K2, V1, V2]
}

func newTextFileReducer[K1, K2 comparable, V1, V2 any](
	reducer ReduceImplementation[K1, K2, V1, V2],
) *textFileReducer[K1, K2, V1, V2] {
	return &textFileReducer[K1, K2, V1, V2]{
		reducer: reducer,
	}
}

func (t *textFileReducer[K1, K2, V1, V2]) Reduce(in Iterator[string], out Emitter[[]Pair[K2, V2]]) error {
	listByKeys := map[K1][]V1{}
	for in.Next() {
		line := in.Value()
		pair, err := t.reducer.LineToPair(line)
		if err != nil {
			return err
		}
		lst := listByKeys[pair.Key]
		listByKeys[pair.Key] = append(lst, pair.Value)
	}
	for k, v := range listByKeys {
		p, err := t.reducer.Reduce(k, v)
		if err != nil {
			return err
		}
		err = out.Emit(p)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReduceTextFile is the entry point you must call
// just provide filenames and split and map functions
func ReduceTextFile[K1, K2 comparable, V1, V2 any](fnIn, fnOut string,
	reducer ReduceImplementation[K1, K2, V1, V2],
) error {
	fileReducer := newTextFileReducer(reducer)
	in, err := NewTextFileIterator(fnIn)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := newTextFileEmitterForReducer[K2, V2](fnOut)
	if err != nil {
		return err
	}
	defer out.Close()
	return fileReducer.Reduce(in, out)
}
