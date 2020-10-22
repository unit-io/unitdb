/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache

import (
	"time"
)

// Options holds the optional blockcache parameters.
type Options struct {
	// MaxBlocks sets Maximum concurrent block caches in blockcache.
	MaxBlocks int

	// DrainInterval sets interval to start blockcache drain if it reaches drain threshold.

	DrainInterval time.Duration

	// DrainFactor sets drain factor to start shrinking datatable if blockcache reaches drain threshold.
	DrainFactor float64

	// The duration for waiting in the queue if blockcache reaches its target size.
	InitialInterval time.Duration

	// RandomizationFactor sets factor to backoff when blockcache reaches target size.
	RandomizationFactor float64

	// MaxElapsedTime sets maximum elapsed time to wait during backoff.
	MaxElapsedTime time.Duration

	// WriteBackOff to turn on Backoff for writes.
	WriteBackOff bool
}

func (src *Options) copyWithDefaults() *Options {
	opts := Options{}
	if src != nil {
		opts = *src
	}

	if opts.MaxBlocks == 0 {
		opts.MaxBlocks = nBlocks
	}

	if opts.DrainInterval == 0 {
		opts.DrainInterval = defaultDrainInterval
	}

	if opts.DrainFactor == 0 {
		opts.DrainFactor = defaultDrainFactor
	}

	if opts.InitialInterval == 0 {
		opts.InitialInterval = defaultInitialInterval
	}

	if opts.RandomizationFactor == 0 {
		opts.RandomizationFactor = defaultRandomizationFactor
	}

	if opts.MaxElapsedTime == 0 {
		opts.MaxElapsedTime = defaultMaxElapsedTime
	}

	return &opts
}
