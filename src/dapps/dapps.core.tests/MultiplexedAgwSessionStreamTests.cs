using System.Text;
using AwesomeAssertions;
using dapps.client.Transport.Agw;

namespace dapps.core.tests;

/// <summary>
/// Unit tests for the multiplexed AGW session stream - the inbound-side
/// glue that lets <c>InboundConnectionHandler</c> consume bytes from a
/// shared AGW socket as if it had a private duplex stream.
/// </summary>
public class MultiplexedAgwSessionStreamTests
{
    [Fact]
    public async Task PushedBytes_AreReadableInOrder()
    {
        var sent = new List<byte[]>();
        var stream = new MultiplexedAgwSessionStream(
            writeOutgoing: (data, _) => { sent.Add(data); return Task.CompletedTask; },
            sendRemoteDisconnect: _ => Task.CompletedTask);

        await stream.PushIncoming("hello "u8.ToArray(), TestContext.Current.CancellationToken);
        await stream.PushIncoming("world\n"u8.ToArray(), TestContext.Current.CancellationToken);

        var buf = new byte[64];
        var n = await stream.ReadAsync(buf.AsMemory(), TestContext.Current.CancellationToken);
        Encoding.UTF8.GetString(buf, 0, n).Should().Be("hello world\n");
    }

    [Fact]
    public async Task Read_BlocksUntilPushArrives_ThenReturns()
    {
        var stream = new MultiplexedAgwSessionStream(
            writeOutgoing: (_, _) => Task.CompletedTask,
            sendRemoteDisconnect: _ => Task.CompletedTask);

        var ct = TestContext.Current.CancellationToken;
        var buf = new byte[16];
        var read = stream.ReadAsync(buf.AsMemory(), ct).AsTask();
        read.IsCompleted.Should().BeFalse("no bytes pushed yet");

        await stream.PushIncoming("abc"u8.ToArray(), ct);
        var n = await read.WaitAsync(TimeSpan.FromSeconds(1), ct);
        Encoding.UTF8.GetString(buf, 0, n).Should().Be("abc");
    }

    [Fact]
    public async Task SignalRemoteDisconnect_DrainsBufferThenReturnsZero()
    {
        var stream = new MultiplexedAgwSessionStream(
            writeOutgoing: (_, _) => Task.CompletedTask,
            sendRemoteDisconnect: _ => Task.CompletedTask);

        var ct = TestContext.Current.CancellationToken;
        await stream.PushIncoming("trailing"u8.ToArray(), ct);
        stream.SignalRemoteDisconnect();

        var buf = new byte[16];
        var n = await stream.ReadAsync(buf.AsMemory(), ct);
        Encoding.UTF8.GetString(buf, 0, n).Should().Be("trailing",
            "bytes pushed before disconnect must still be readable");

        var n2 = await stream.ReadAsync(buf.AsMemory(), ct);
        n2.Should().Be(0, "subsequent reads see EOF after the buffer drains");
    }

    [Fact]
    public async Task Write_InvokesCallbackWithExactBytes()
    {
        var sent = new List<byte[]>();
        var stream = new MultiplexedAgwSessionStream(
            writeOutgoing: (data, _) => { sent.Add(data); return Task.CompletedTask; },
            sendRemoteDisconnect: _ => Task.CompletedTask);

        await stream.WriteAsync("DAPPSv1>\n"u8.ToArray().AsMemory(),
            TestContext.Current.CancellationToken);
        await stream.FlushAsync(TestContext.Current.CancellationToken);

        sent.Should().HaveCount(1);
        Encoding.UTF8.GetString(sent[0]).Should().Be("DAPPSv1>\n");
    }

    [Fact]
    public async Task DisposeAsync_FiresRemoteDisconnectCallback()
    {
        var disconnectFired = false;
        var stream = new MultiplexedAgwSessionStream(
            writeOutgoing: (_, _) => Task.CompletedTask,
            sendRemoteDisconnect: _ => { disconnectFired = true; return Task.CompletedTask; });

        await stream.DisposeAsync();
        disconnectFired.Should().BeTrue(
            "disposing the stream should signal a 'd' frame so BPQ tears down the L2 link rather than leaving it half-up");
    }

    [Fact]
    public async Task DisposeAsync_AfterRemoteDisconnect_DoesNotSendDisconnect()
    {
        var disconnectCount = 0;
        var stream = new MultiplexedAgwSessionStream(
            writeOutgoing: (_, _) => Task.CompletedTask,
            sendRemoteDisconnect: _ => { Interlocked.Increment(ref disconnectCount); return Task.CompletedTask; });
        var ct = TestContext.Current.CancellationToken;
        var read = stream.ReadAsync(new byte[16].AsMemory(), ct).AsTask();
        read.IsCompleted.Should().BeFalse();

        stream.SignalRemoteDisconnect();
        stream.SignalRemoteDisconnect();
        await Task.WhenAll(Enumerable.Range(0, 16)
            .Select(_ => Task.Run(async () => await stream.DisposeAsync(), ct)))
            .WaitAsync(TimeSpan.FromSeconds(5), ct);

        (await read.WaitAsync(TimeSpan.FromSeconds(5), ct)).Should().Be(0);
        disconnectCount.Should().Be(0,
            "the peer already hung up, so BPQ has released the session; a late 'd' is addressed by " +
            "callsign pair only and would tear down a new session that reused the same pair");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisposeAsync_WhileDisconnectPending_ClosesStreamAndSendsOnlyOnce(bool remoteCloses)
    {
        var disconnectCount = 0;
        var writeCount = 0;
        var releaseDisconnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stream = new MultiplexedAgwSessionStream(
            writeOutgoing: (_, _) => { Interlocked.Increment(ref writeCount); return Task.CompletedTask; },
            sendRemoteDisconnect: _ =>
            {
                Interlocked.Increment(ref disconnectCount);
                return releaseDisconnect.Task;
            });
        var ct = TestContext.Current.CancellationToken;
        var read = stream.ReadAsync(new byte[16].AsMemory(), ct).AsTask();
        var disposal = stream.DisposeAsync().AsTask();

        try
        {
            disposal.IsCompleted.Should().BeFalse();
            disconnectCount.Should().Be(1);
            if (remoteCloses) stream.SignalRemoteDisconnect();

            await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(async () =>
            {
                stream.Dispose();
                await stream.DisposeAsync();
            }, ct))).WaitAsync(TimeSpan.FromSeconds(5), ct);

            (await read.WaitAsync(TimeSpan.FromSeconds(5), ct)).Should().Be(0);
            stream.CanRead.Should().BeFalse();
            stream.CanWrite.Should().BeFalse();
            await Assert.ThrowsAsync<IOException>(() => stream.WriteAsync("late"u8.ToArray().AsMemory(), ct).AsTask());
            writeCount.Should().Be(0);
            disconnectCount.Should().Be(1);
        }
        finally
        {
            releaseDisconnect.TrySetResult();
            await disposal.WaitAsync(TimeSpan.FromSeconds(5), ct);
        }
    }

    [Fact]
    public async Task DisposeAsync_ReenteredFromDisconnectCallback_SendsOnlyOnce()
    {
        var disconnectCount = 0;
        MultiplexedAgwSessionStream? stream = null;
        stream = new MultiplexedAgwSessionStream(
            writeOutgoing: (_, _) => Task.CompletedTask,
            sendRemoteDisconnect: async _ =>
            {
                Interlocked.Increment(ref disconnectCount);
                stream!.SignalRemoteDisconnect();
                stream.Dispose();
                await stream.DisposeAsync();
            });

        await stream.DisposeAsync().AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        disconnectCount.Should().Be(1);
        stream.CanRead.Should().BeFalse();
        stream.CanWrite.Should().BeFalse();
    }

    [Fact]
    public async Task ConcurrentRemoteDisconnectAndDispose_SendAtMostOneDisconnect()
    {
        var ct = TestContext.Current.CancellationToken;
        for (var i = 0; i < 100; i++)
        {
            var disconnectCount = 0;
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var stream = new MultiplexedAgwSessionStream(
                writeOutgoing: (_, _) => Task.CompletedTask,
                sendRemoteDisconnect: _ => { Interlocked.Increment(ref disconnectCount); return Task.CompletedTask; });
            var closers = Enumerable.Range(0, 8).Select(async n =>
            {
                await start.Task;
                if (n % 2 == 0) stream.SignalRemoteDisconnect();
                else await stream.DisposeAsync();
            }).ToArray();

            start.SetResult();
            await Task.WhenAll(closers).WaitAsync(TimeSpan.FromSeconds(5), ct);

            disconnectCount.Should().BeInRange(0, 1);
            stream.CanRead.Should().BeFalse();
            stream.CanWrite.Should().BeFalse();
            (await stream.ReadAsync(new byte[16].AsMemory(), ct).AsTask()
                .WaitAsync(TimeSpan.FromSeconds(5), ct)).Should().Be(0);
        }
    }
}
