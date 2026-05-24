using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

public sealed class TcpLogServer
{
    private readonly TcpListener _listener;
    private readonly ChannelWriter<LogMessage> _writer;

    private readonly List<Task> _activeClients = [];
    private readonly object _clientsLock = new();

    private static readonly UTF8Encoding Utf8Strict = new UTF8Encoding(false, true);

    public TcpLogServer(IPAddress ipAddress, int port, ChannelWriter<LogMessage> writer)
    {
        _listener = new TcpListener(ipAddress, port);
        _writer = writer;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _listener.Start();

        Console.WriteLine("Listening on port 5000");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(cancellationToken);

                client.NoDelay = true;

                client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

                var task = HandleClientAsync(client, cancellationToken);

                lock (_clientsLock)
                {
                    _activeClients.Add(task);
                }

                _ = task.ContinueWith(_ =>
                {
                    lock (_clientsLock)
                    {
                        _activeClients.Remove(task);
                    }

                }, TaskScheduler.Default);
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Server shutdown requested.");
        }
        finally
        {
            _listener.Stop();
        }
    }

    public async Task StopAsync()
    {
        Task[] clients;

        lock (_clientsLock)
        {
            clients = _activeClients.ToArray();
        }

        Console.WriteLine($"Waiting for {clients.Length} active clients...");

        if (clients.Length > 0)
        {
            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(30));

            var allClientsTask = Task.WhenAll(clients);

            var completed = await Task.WhenAny(allClientsTask, timeoutTask);

            if (completed == timeoutTask)
            {
                Console.WriteLine("Timeout waiting clients.");
            }
        }

        _writer.TryComplete();
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var endpoint = client.Client.RemoteEndPoint?.ToString() ?? "Unknown";

        Console.WriteLine($"Client connected: {endpoint}");

        using (client)
        using (var stream = client.GetStream())
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(8192);

            var decoder = Utf8Strict.GetDecoder();

            char[] charBuffer = ArrayPool<char>.Shared.Rent(8192);

            var sb = new StringBuilder(4096);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

                    timeoutCts.CancelAfter(TimeSpan.FromMinutes(2));

                    int bytesRead;

                    try
                    {
                        bytesRead = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), timeoutCts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        Console.WriteLine($"Timeout: {endpoint}");
                        break;
                    }

                    if (bytesRead == 0)
                    {
                        Console.WriteLine($"Disconnected: {endpoint}");
                        break;
                    }

                    try
                    {
                        int charsDecoded = decoder.GetChars(buffer, 0, bytesRead, charBuffer, 0, flush: false);

                        sb.Append(charBuffer, 0, charsDecoded);
                    }
                    catch (DecoderFallbackException)
                    {
                        await _writer.WriteAsync(new LogMessage(DateTime.UtcNow, endpoint, "[INVALID UTF8 PAYLOAD]"), cancellationToken);

                        decoder.Reset();
                        sb.Clear();

                        continue;
                    }

                    ProcessMessages(sb, endpoint, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"Client error [{endpoint}]: {ex.Message}");
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
                ArrayPool<char>.Shared.Return(charBuffer);

                Console.WriteLine($"Connection closed: {endpoint}");
            }
        }
    }

    private void ProcessMessages(
        StringBuilder sb,
        string endpoint,
        CancellationToken cancellationToken)
    {
        int start = 0;

        while (true)
        {
            int newLineIndex = -1;

            for (int i = start; i < sb.Length; i++)
            {
                if (sb[i] == '\n')
                {
                    newLineIndex = i;
                    break;
                }
            }

            if (newLineIndex < 0)
                break;

            int length = newLineIndex - start;

            if (length > 0 &&
                sb[newLineIndex - 1] == '\r')
            {
                length--;
            }

            if (length > 0)
            {
                string line =
                    sb.ToString(start, length);

                _writer.WriteAsync(
                    new LogMessage(
                        DateTime.UtcNow,
                        endpoint,
                        line),
                    cancellationToken)
                    .AsTask()
                    .GetAwaiter()
                    .GetResult();
            }

            start = newLineIndex + 1;
        }

        if (start > 0)
        {
            sb.Remove(0, start);
        }

        if (sb.Length > 65536)
        {
            _writer.WriteAsync(
                new LogMessage(
                    DateTime.UtcNow,
                    endpoint,
                    "[BUFFER OVERFLOW]"),
                cancellationToken)
                .AsTask()
                .GetAwaiter()
                .GetResult();

            sb.Clear();
        }
    }
}