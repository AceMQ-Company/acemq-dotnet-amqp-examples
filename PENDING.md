# Waiting on the next release

This branch holds examples that are finished and verified, but that need a fix
which is on `main` in [acemq-dotnet-amqp](https://github.com/AceMQ-Company/acemq-dotnet-amqp)
and not yet in a released package.

The examples on `main` resolve released packages on purpose — an example that
needs an unreleased fix would be a red build for everyone who clones the
repository, so it waits here instead.

| Example | Needs |
|---|---|
| `basic/05-transactional-outbox-{csharp,vbnet}` | `OutboxRecord.For`, and the relay publishing a stored payload verbatim rather than encoding it a second time |
| `basic/06-serialization-{csharp,vbnet}` | the consumer passing a message's content type to the codec, so a composite reads a queue holding two formats |

Both pairs were run against a broker with the library built from source, in C#
and in VB.NET. When a release carries the fixes, merge this branch into `main`,
add the rows to the README table, and delete this file.
