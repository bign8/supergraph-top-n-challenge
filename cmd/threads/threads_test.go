package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkProcessRows(b *testing.B) {
	p, err := newProcessor()
	if err != nil {
		b.Skip(err)
		b.SkipNow()
	}
	benchmark(b, p.processRows)
}

func BenchmarkProcessArray(b *testing.B) {
	p, err := newProcessor()
	if err != nil {
		b.Skip(err)
		b.SkipNow()
	}
	benchmark(b, p.processArray)
}

func benchmark(b *testing.B, subject func(limit int32) ([]int32, error)) {
	b.Run(`2`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := subject(2)
			require.NoError(b, err)
			assert.Len(b, rows, 2)
		}
	})

	b.Run(`20`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := subject(20)
			require.NoError(b, err)
			assert.Len(b, rows, 20)
		}
	})

	b.Run(`100`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := subject(100)
			require.NoError(b, err)
			assert.Len(b, rows, 100)
		}
	})

	b.Run(`1000`, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := subject(1000)
			require.NoError(b, err)
			assert.Len(b, rows, 1000)
		}
	})
}
