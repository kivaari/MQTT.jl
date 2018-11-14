# commands
const CONNECT = 0x10
const CONNACK = 0x20
const PUBLISH = 0x30
const PUBACK = 0x40
const PUBREC = 0x50
const PUBREL = 0x60
const PUBCOMP = 0x70
const SUBSCRIBE = 0x80
const SUBACK = 0x90
const UNSUBSCRIBE = 0xA0
const UNSUBACK = 0xB0
const PINGREQ = 0xC0
const PINGRESP = 0xD0
const DISCONNECT = 0xE0
const QOS_1_BYTE = 0x02
const QOS_2_BYTE = 0x04

mutable struct TestFileHandler <: IO
    out_channel::Channel{UInt8}
    in_channel::Channel{UInt8}
    closenotify::Condition
    new_input::Condition
    closed::Bool
end

function read(fh::TestFileHandler, t)
    return take!(fh.in_channel)
end

function read(fh::TestFileHandler, t::Type{UInt8})
    return take!(fh.in_channel)
end

function read(fh::TestFileHandler, length::Integer)
    data = Vector{UInt8}()
    for i = 1:length
        append!(data, take!(fh.in_channel))
    end
    return data
end

function close(fh::TestFileHandler)
    fh.closed = true
    notify(fh.closenotify)
end

function write(fh::TestFileHandler, data::UInt8)
    put!(fh.out_channel, data)
end

function put(fh::TestFileHandler, data::Array{UInt8})
  for i in data
      put!(fh.in_channel, i)
  end
end

function put(fh::TestFileHandler, data::SubArray)
  for i in data
      put!(fh.in_channel, i)
  end
end

function put(fh::TestFileHandler, data::UInt16)
    buffer = PipeBuffer()
    write(buffer, data)
    put(fh, reverse(read(buffer), 1)) #TODO use take! instead?
end

function put(fh::TestFileHandler, data::UInt8)
  put!(fh.in_channel, data)
end

function put_from_file(fh::TestFileHandler, filename)
  put(fh, read_all_to_arr(filename))
end

function get_mid_index(data::Array{UInt8})
  cmd = data[1] & 0xF0
  if cmd == CONNACK || cmd == PINGRESP || cmd == CONNECT || cmd == DISCONNECT || cmd == PINGREQ
    return -1
  elseif cmd == PUBLISH
    qos = data[1] & 0x06
    if qos == QOS_1_BYTE || qos == QOS_2_BYTE
      buffer = PipeBuffer()
      write(buffer, data[4])
      write(buffer, data[3])
      topic_len = read_len(buffer)
      return 5 + topic_len
    else
      # QOS_0 has no message id
      return -1
    end
  else
    # all other packets have m_id as their 3rd and 4th byte
    return 3
  end
end

function put_from_file(fh::TestFileHandler, filename, messageId::UInt16)
  data = read_all_to_arr(filename)
  mid_index = get_mid_index(data)
  if mid_index > 0
    put(fh, view(data, 1:mid_index - 1))
    put(fh, messageId)
    put(fh, view(data, mid_index + 2:length(data)))
  else
    put(fh, data)
  end
end

function read_all_to_arr(filename)
    file = open(filename, "r")
    data = Vector{UInt8}()
    while !eof(file)
        append!(data, read(file, UInt8))
    end
    close(file)
    return data
end

function connect(host::AbstractString, port::Integer)
    th = TestFileHandler(Channel{UInt8}(256), Channel{UInt8}(256), Condition(), Condition(), false)
    put_from_file(th, joinpath(datadir, "input", "connack.dat"))
    return th
end
