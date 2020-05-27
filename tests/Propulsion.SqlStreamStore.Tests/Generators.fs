module Propulsion.Sql.Tests.Generators

open System

open FsCheck
open SqlStreamStore.Streams

open Newtonsoft.Json
open Propulsion.SqlStreamStore.StreamReader
open Propulsion.Streams

let streamId =
    gen {
        let! values = Gen.nonEmptyListOf (Arb.generate<Guid>)
        let! guid = Gen.elements values
        return StreamId(string guid)
    }

type EventType =
    | Event1
    | Event2
    | Event3

type Message =
    {
        data: string
    }
    member this.Serialize() =
        JsonConvert.SerializeObject(this)

    static member Deserialize (msg : string) =
        JsonConvert.DeserializeObject<Message>(msg)

let message =
    gen {
        let! data = Arb.generate<Guid>
        let msg = { data = string data }

        let! typ = Arb.generate<EventType>

        return NewStreamMessage(Guid.NewGuid(), string typ, msg.Serialize())
    }

let streams =
    gen {
        let! values =
            Gen.listOf (Gen.zip streamId message)

        return values
    }

let batches : Gen<InternalBatch array> =
    gen {
        let! numberOfBatches =
            Gen.choose(0, 10)

        let! lengths =
            Gen.listOfLength numberOfBatches (Arb.generate<PositiveInt>)

        let batches = ResizeArray<InternalBatch>()

        let mutable pos = 0

        for len in lengths do

            let firstPos = int64 pos

            let events : StreamEvent<byte[]> array =
                [|
                    for i in 0 .. len.Get - 1 do
                       let event =
                            FsCodec.Core.TimelineEvent.Create (
                                int64 pos,
                                "type",
                                null,
                                null,
                                Guid.NewGuid(),
                                timestamp = DateTimeOffset.UtcNow
                            )

                       pos <- pos + 1

                       yield { stream = StreamName.internalParseSafe "test"; event = event }
                |]

            let! isEnd = Arb.generate<bool>

            let batch : InternalBatch =
                {
                    firstPosition = firstPos
                    lastPosition = int64 (pos - 1)
                    isEnd = isEnd
                    messages = events
                }

            batches.Add(batch)

        return Seq.toArray batches
    }



