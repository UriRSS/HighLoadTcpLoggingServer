using System.Net;
using System.Threading.Channels;

Console.WriteLine("High-load async TCP logging server started.");

var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\nShutdown requested...");
};

var channel = Channel.CreateBounded<LogMessage>(
    new BoundedChannelOptions(100_000)
    {
        SingleReader = true,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.Wait
    });

var server = new TcpLogServer( IPAddress.Any, 5000, channel.Writer);

var logger = new FileLoggerService( channel.Reader, "logs.txt");

var serverTask = server.StartAsync(cts.Token);
var loggerTask = logger.RunAsync(cts.Token);

await Task.WhenAny(serverTask, loggerTask);

await server.StopAsync();

await loggerTask;

Console.WriteLine("Server stopped gracefully.");

public sealed record LogMessage(DateTime Timestamp, string RemoteEndPoint, string Message);