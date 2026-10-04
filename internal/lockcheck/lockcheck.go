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

// Package lockcheck checks that locks are taken in the order of their
// ranks: a goroutine holding a lock takes only locks of higher rank. Two
// goroutines taking two locks in opposite orders can deadlock, and do so
// rarely enough that tests miss it; a goroutine taking them out of order is
// found by any test that runs the path once.
//
// Built with the lockcheck tag, Acquire panics on a lock taken out of order;
// otherwise Acquire and Release do nothing, and cost nothing.
package lockcheck
