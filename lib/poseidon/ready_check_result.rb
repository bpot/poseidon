module Poseidon
  ReadyCheckResult = Struct.new(:ready_nodes, :next_ready_check_delay_ms, :unknown_leaders_exist)
end
