using System.Text;
using System.Threading.Channels;

public sealed class FileLoggerService
{
    private readonly ChannelReader<LogMessage> _reader;
    private readonly string _filePath;

    public FileLoggerService(ChannelReader<LogMessage> reader, string filePath)
    {
        _reader = reader;
        _filePath = filePath;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(_filePath, FileMode.Append, FileAccess.Write, FileShare.Read, bufferSize: 65536, useAsync: true);

        await using var writer = new StreamWriter(stream, Encoding.UTF8)
        {
            AutoFlush = false
        };

        int pendingMessages = 0;

        try
        {
            await foreach (var log in _reader.ReadAllAsync(cancellationToken))
            {
                await writer.WriteAsync('[');

                await writer.WriteAsync(log.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff"));

                await writer.WriteAsync("] [");

                await writer.WriteAsync(log.RemoteEndPoint);

                await writer.WriteAsync("] ");

                await writer.WriteLineAsync(log.Message);

                pendingMessages++;

                if (pendingMessages >= 100)
                {
                    await writer.FlushAsync();
                    pendingMessages = 0;
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Logger stopping...");
        }
        finally
        {
            await writer.FlushAsync();

            Console.WriteLine("Logger finished.");
        }
    }
}