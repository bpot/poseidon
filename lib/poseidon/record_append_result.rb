module Poseidon
  RecordAppendResult = Struct.new(:future, :batch_is_full, :new_batch_created)
end
