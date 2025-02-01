package mapreduce

// Iterator allow to iterate over an input document
type Iterator[V any] interface {
	Next() bool
	Value() V
	Error() error
	Close() error
}

// Emitter allow to write in an output document
type Emitter[V any] interface {
	Emit(V) error
	Close() error
}

// DocSplitter used to split an input document
type DocSplitter[I any, O any] interface {
	Split(Iterator[I], []Emitter[O])
}

// Mapper iterate over a document emitting pairs
type Mapper[I any, O any] interface {
	Map(Iterator[I], Emitter[O]) error
}

// Reducer reduce an iterator
type Reducer[I any, O any] interface {
	Reduce(Iterator[I], Emitter[O]) error
}

// Pair of key and value
type Pair[K comparable, V any] struct {
	Key   K
	Value V
}

// RecordSplitter split a record into many parts
type RecordSplitter[T any] func(T) []T

// RecordMapper take an input and extract a Pair
type RecordMapper[I any, K comparable, V any] func(I) Pair[K, V]

// HashFunc calculate a hash for a given key, is used by shuffler
type HashFunc[I comparable, O comparable] func(key I) O
