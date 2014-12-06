# -*- coding: utf-8 -*-

require 'spec_helper'
include Protocol

RSpec.describe RequestBuffer do
  subject(:buffer) { Poseidon::Protocol::RequestBuffer.new }

  it 'appends UTF-8 strings' do
    expect do
      str = 'hello ümlaut'
      buffer.append(str)
      buffer.append(str.force_encoding(Encoding::BINARY))
    end.to_not raise_error
  end
end
