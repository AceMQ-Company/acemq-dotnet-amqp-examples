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

using AceMq.Amqp;

namespace EventSourcedLedger;

// Where transfers are asked for, and where refusals are noticed.
//
// Deliberately thin. Everything a ledger is careful about happens in the writer;
// this module makes the shape obvious -- a transfer is a command, sent to a queue,
// which may be refused, and a refusal is a normal outcome rather than an error.
//
// Events are named in the past tense and cannot be argued with; commands are
// requests and can be turned down. Systems that blur the two end up publishing
// TransferMade before knowing whether it was, and then need a second event to
// take it back.
public sealed class TransferGateway
{
    private readonly IPublisher<Transfer> _requests;
    private readonly List<TransferRejected> _refused = new();

    private TransferGateway(AceMqConnection mq) =>
        _requests = mq.Publisher<Transfer>(Ledger.Exchange, Ledger.TransferRequestedKey);

    public static async Task<TransferGateway> StartAsync(AceMqConnection mq)
    {
        var gateway = new TransferGateway(mq);
        await mq.ConsumeAsync<TransferRejected>(Ledger.Rejections, message =>
        {
            lock (gateway._refused) gateway._refused.Add(message.Payload);
            return Task.FromResult(Ack.Accept());
        });
        return gateway;
    }

    /// <summary>Asks for money to move.</summary>
    /// <returns>The transfer id, which correlates the command with both entries and any refusal.</returns>
    public async Task<string> RequestAsync(string from, string to, long amountMinor, string description)
    {
        var transferId = "T-" + Guid.NewGuid().ToString("N")[..8];
        await _requests.SendAsync(
            new Transfer(transferId, from, to, amountMinor, description),
            Envelope.Of("Transfer").CorrelationId(transferId).Build());
        return transferId;
    }

    /// <summary>The transfers the ledger refused, and why.</summary>
    public IReadOnlyList<TransferRejected> Refused
    {
        get
        {
            lock (_refused) return _refused.ToArray();
        }
    }
}
