package dataset

import "github.com/in4it/gomap/pkg/types"

type Actions interface {
	FlatMap(fn types.FlatMapFunction) Actions
	Map(fn types.MapFunction) Actions
	MapToKV(fn types.MapToKVFunction) Actions
	ReduceByKey(fn types.ReduceByKeyFunction) Actions
}
