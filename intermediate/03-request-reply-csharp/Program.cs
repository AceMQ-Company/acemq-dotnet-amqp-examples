// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Asking a question over a queue and waiting for the answer, in C#.
//
//   docker compose up -d
//   dotnet run --project intermediate/03-request-reply-csharp
//
// Three things happen here: a round trip through a Responder, the same round
// trip served by hand so the reply address can be read off the message, and a
// request nobody answers.

using AceMq.Amqp;
using AceMq.Amqp.RabbitMq;

public sealed class QuoteRequest
{
    public string Symbol { get; set; } = "";
}

public sealed class Quote
{
    public string Symbol { get; set; } = "";
    public long Pence { get; set; }
}

public static class Program
{
    // Queue names of this example's own. Every example on this repository's CI
    // broker uses its own, because two of them declaring one name with
    // different settings is a PRECONDITION_FAILED rather than a coincidence.
    private const string Served = "quotes-served";
    private const string ByHand = "quotes-by-hand";
    private const string Unanswered = "quotes-unanswered";

    public static async Task<int> Main()
    {
        Transports.Register(new RabbitMqTransport());

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = cancellation.Token;

        using var mq = await AceMqConnection.ConnectAsync(
            ConnectionConfig.ForUrl(BrokerUrl())
                .ClientName("examples/03-request-reply-csharp")
                .Build());

        await mq.DeclareQueueAsync(Served);
        await mq.DeclareQueueAsync(ByHand);
        // Declared and deliberately never consumed. A request sent here is the
        // one that times out.
        await mq.DeclareQueueAsync(Unanswered);

        // One reply queue for the whole requester, not one per request. A queue
        // per call costs the broker a declare and a delete every time, which is
        // the difference between request/reply being usable at rate and being a
        // curiosity. Replies are matched by correlation id instead.
        using var asking = await mq.RequesterAsync();
        Console.WriteLine($"replies come back on {asking.ReplyQueue}");

        // ---- 1. the round trip -------------------------------------------
        //
        // A Responder is a consumer that publishes the handler's return value
        // back to whoever asked. Nothing in the handler mentions a reply queue.
        using var answering = await mq.RespondAsync<QuoteRequest, Quote>(
            Served,
            request => Task.FromResult(new Quote { Symbol = request.Symbol, Pence = 1234 }));

        var quote = await asking.RequestAsync<QuoteRequest, Quote>(
            "", Served, new QuoteRequest { Symbol = "ACME" },
            TimeSpan.FromSeconds(20), token);

        Console.WriteLine($"asked for ACME and got {quote.Symbol} at {quote.Pence}p");
        Check(quote.Symbol == "ACME", $"the reply was for {quote.Symbol}, not ACME");
        Check(quote.Pence == 1234, $"the reply carried {quote.Pence}, not 1234");

        // Read straight away, with no wait. The counter is incremented before
        // the reply is published, so a caller holding its answer can never see
        // a count that has not caught up. Writing this example is what found
        // the bug: it used to be counted afterwards, and asserting it here
        // failed about half the time.
        Check(answering.Answered == 1,
            $"the responder answered {answering.Answered} times, not once");

        // ---- 2. where the reply address travels --------------------------
        //
        // This is what 0.5.0 fixed, and it is invisible from inside a
        // Responder. .NET and Java put the address in AMQP's own reply-to
        // property; Go, Python and Ruby put it in an application header. A .NET
        // requester and a Go responder therefore could not talk at all. Every
        // library now writes both and reads the header first, so this consumer
        // -- standing in for a responder written in another language -- can pick
        // up either one and answer.
        var seen = new TaskCompletionSource<(string? Header, string? Property)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        using var byHand = await mq.ConsumeAsync<QuoteRequest>(ByHand, async message =>
        {
            // Requester.ReplyToHeader is "acemq-reply-to". It deliberately does
            // not carry the x-acemq- prefix: that namespace is the engine's and
            // is stripped before a handler sees it, so a responder could never
            // read a reply address written there.
            message.Headers.TryGetValue(Requester.ReplyToHeader, out var header);
            seen.TrySetResult((header?.ToString(), message.ReplyTo));

            // Answering by hand is three lines: publish to the address on the
            // default exchange, with the request's id as the correlation. That
            // is the whole contract a Responder implements.
            var envelope = Envelope.Of(message.Envelope.Type)
                .CorrelationId(message.Envelope.Id)
                .CausationId(message.Envelope.Id)
                .Build();
            await mq.Publisher<Quote>("", message.ReplyTo!)
                .SendAsync(new Quote { Symbol = message.Payload.Symbol, Pence = 4321 }, envelope);

            return Ack.Accept();
        });

        var byHandQuote = await asking.RequestAsync<QuoteRequest, Quote>(
            "", ByHand, new QuoteRequest { Symbol = "GLOBEX" },
            TimeSpan.FromSeconds(20), token);

        var addresses = await seen.Task.WaitAsync(token);
        Console.WriteLine($"  {Requester.ReplyToHeader} header: {addresses.Header}");
        Console.WriteLine($"  AMQP reply-to property:  {addresses.Property}");
        Check(addresses.Header == asking.ReplyQueue,
            $"the header said {addresses.Header}, not {asking.ReplyQueue}");
        Check(addresses.Property == asking.ReplyQueue,
            $"the property said {addresses.Property}, not {asking.ReplyQueue}");
        Check(byHandQuote.Pence == 4321,
            $"the hand-written reply carried {byHandQuote.Pence}, not 4321");

        // ---- 3. nobody answers -------------------------------------------
        //
        // The interesting failure. A request that is never answered does not
        // hang for ever and does not fail silently: it throws once the timeout
        // is up, and the requester counts it.
        var timedOut = false;
        try
        {
            await asking.RequestAsync<QuoteRequest, Quote>(
                "", Unanswered, new QuoteRequest { Symbol = "NOBODY" },
                TimeSpan.FromSeconds(2), token);
        }
        catch (RequestTimedOutException failure)
        {
            timedOut = true;
            Console.WriteLine($"gave up: {failure.Message}");
        }

        Check(timedOut, "a request to a queue nobody serves came back anyway");
        Check(asking.TimedOut == 1, $"{asking.TimedOut} requests timed out, not one");

        // A reply that turns up after its caller has given up is counted and
        // dropped rather than handed to whoever asks next. Handing a late
        // answer to the wrong caller is worse than no answer, and it is exactly
        // what happens when a shared reply queue is read without matching on
        // the correlation id.
        Console.WriteLine(
            $"answered {answering.Answered}, timed out {asking.TimedOut}, " +
            $"unmatched replies {asking.Unmatched}");

        // The example cleans up after itself so a second run reports the same
        // numbers as the first. A service would leave the queues alone. The
        // reply queue needs no cleaning: it carries x-expires, so a process
        // killed without disposing leaves nothing behind for an afternoon.
        answering.Dispose();
        byHand.Dispose();
        foreach (var queue in new[] { Served, ByHand, Unanswered })
        {
            await mq.DeleteQueueAsync(queue);
            await mq.DeleteQueueAsync(queue + ".dlq");
            await mq.DeleteQueueAsync(queue + ".parked");
        }

        return 0;
    }

    // An example that prints the right answer whatever happened is an example
    // that cannot fail, and CI running it proves nothing. This throws, which
    // makes the process exit non-zero.
    private static void Check(bool held, string wrong)
    {
        if (!held) throw new InvalidOperationException(wrong);
    }

    private static string BrokerUrl() =>
        Environment.GetEnvironmentVariable("ACEMQ_URL") is { Length: > 0 } url
            ? url
            : "amqp://guest:guest@localhost:5672/";
}
