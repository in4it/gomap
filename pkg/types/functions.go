package types

// FlatMapFunction can be used to flatten data. It takes a RawInput argument and outputs one or more RawOutput elements
type FlatMapFunction func(RawInput) []RawOutput

// MapFunction can be used to transform data, using a RawInput argument and outputting a RawOutput
type MapFunction func(RawInput) RawOutput

// MapToKVFunction can be used to convert data to a key-value pair. It takes one RawInput and outputs a Key and Value in RawOutput format
type MapToKVFunction func(RawInput) (RawOutput, RawOutput)

// ReduceByKeyFunction is a function to reduce data. It takes 2 RawInputs and outputs a RawOutput
// It is used to take 2 values (for example a & b), and reduce them to a new output (for example c)
// In the wordcount example, the input values are integers and the output is the sum (c = a + b)
type ReduceByKeyFunction func(RawInput, RawInput) RawOutput

// FilterFunction filters the data, taking a RawInput argument and outputting a boolean
// true includes the data in the dataset
type FilterFunction func(RawInput) bool

// ForeachFunction can be used to iterate over the result dataset
// The Foreach Function will iterate over every unique key
type ForeachFunction func(RawOutput, RawOutput)
