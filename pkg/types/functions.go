package types

type FlatMapFunction func(RawInput) []RawOutput
type MapFunction func(RawInput) RawOutput
type MapToKVFunction func(RawInput) (RawOutput, RawOutput)
type ReduceByKeyFunction func(RawInput, RawInput) RawOutput

type ForeachFunction func(RawOutput, RawOutput)
