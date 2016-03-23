// Copyright 2010 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: onufry@google.com (Jakub Onufry Wojtaszczyk)

#include "supersonic/base/infrastructure/double_buffered_block.h"

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"

namespace supersonic {

Block* DoubleBufferedBlock::switch_block() {
  give_first_block_ = !give_first_block_;
  Block* current_block = (give_first_block_) ? &block_1_ : &block_2_;
  current_block->ResetArenas();
  return current_block;
}

Block* DoubleBufferedBlock::get_block() {
  return (give_first_block_) ? &block_1_ : &block_2_;
}

bool DoubleBufferedBlock::Reallocate(rowcount_t row_capacity) {
  DCHECK_GE(row_capacity, 0LL);
  return block_1_.Reallocate(row_capacity) && block_2_.Reallocate(row_capacity);
}

}  // namespace supersonic
