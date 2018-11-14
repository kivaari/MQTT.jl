using Test
using MQTT
using Random

import Base: read, write, close
import MQTT: read_len, Message
import Sockets: connect

const datadir = joinpath(dirname(@__FILE__), "data")

include("smoke.jl")
include("mocksocket.jl")
include("packet.jl")
include("unittests.jl")
