package mapreduce

func ShuffleTextFiles(
	in []string,
	prefix string,
	parts int,
	hash HashFunc[string, int],
) ([]string, error) {
	iters, err := OpenTextFileLineIterators(in)
	if err != nil {
		return nil, err
	}
	emitters, names, err := OpenTextFileLineEmitters(prefix, parts)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = CloseTextFileLineIterators(iters, in)
		_ = CloseTextFileLineEmitters(emitters, names)
	}()
	for _, iter := range iters {
		for iter.Next() {
			key := iter.Value()
			i := hash(key)
			err = emitters[i].Emit(key)
			if err != nil {
				return nil, err
			}
		}
	}
	return names, nil
}
