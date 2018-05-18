module Jet.ConfluentKafka.Tests.Main

#nowarn "988" // Main module of program is empty

// required for netcoreapp builds
#if NETCOREAPP2_0
[<EntryPoint>] let main _ = 0
#endif