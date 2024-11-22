using System;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;

class Program
{
    const string connectionString = "Data Source=thinker;Initial Catalog=LSRProd;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";

    static void Main()
    {
        string logFilePath = @"C:\Development\Utils\APILogsToDB\Logs\log-2024-11-21.txt";
        string tableName = "LogEntries";

        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            var lines = File.ReadAllLines(logFilePath);

            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                try
                {
                    var logEntry = ParseLogEntry(line);

                    if (logEntry != null)
                    {
                        var query = $"INSERT INTO {tableName} (DateTime, Action, Parameter, URL, IP, Duration) " +
                                    "VALUES (@DateTime, @Action, @Parameter, @URL, @IP, @Duration)";

                        using (var command = new SqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@DateTime", logEntry.DateTime);
                            command.Parameters.AddWithValue("@Action", logEntry.Action ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@Parameter", logEntry.Parameter ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@URL", logEntry.URL ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@IP", logEntry.IP ?? (object)DBNull.Value);
                            command.Parameters.AddWithValue("@Duration", logEntry.Duration ?? (object)DBNull.Value);

                            command.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error processing line: {line}");
                    Console.WriteLine($"Error: {ex.Message}");
                }
            }
        }

        Console.WriteLine("Log data inserted successfully.");
    }

    static LogEntry ParseLogEntry(string line)
    {
        var entry = new LogEntry();

        try
        {
            var timestampEnd = line.IndexOf("]");
            var timestampWithLevel = line.Substring(1, timestampEnd - 1);
            var timestampParts = timestampWithLevel.Split(' ');
            var timestampString = timestampParts[0] + " " + timestampParts[1];
            var logLevel = timestampParts[2]; // Extract log level (e.g., INF)

            var utcDateTime = DateTime.ParseExact(timestampString, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            entry.DateTime = TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"));

            var actionStart = timestampEnd + 2;
            var actionEnd = line.IndexOf(":", actionStart);
            var actionText = line.Substring(actionStart, actionEnd - actionStart).Trim();
            entry.Action = $"{logLevel} {actionText}";

            // Extract parameter, URL, IP, and duration
            var remainingLine = line.Substring(actionEnd + 1).Trim();

            if (remainingLine.Contains("\""))
            {
                var urlStart = remainingLine.IndexOf("\"") + 1;
                var urlEnd = remainingLine.IndexOf("\"", urlStart);
                entry.URL = remainingLine.Substring(urlStart, urlEnd - urlStart);

                remainingLine = remainingLine.Substring(urlEnd + 1).Trim();
            }

            if (remainingLine.Contains(" from "))
            {
                var ipStart = remainingLine.IndexOf(" from ") + 6;
                var ipEnd = remainingLine.IndexOf(" ", ipStart);
                entry.IP = ipEnd > 0 ? remainingLine.Substring(ipStart, ipEnd - ipStart) : remainingLine.Substring(ipStart);
            }

            if (remainingLine.Contains("Duration:"))
            {
                var durationStart = remainingLine.IndexOf("Duration:") + 9;
                var durationEnd = remainingLine.IndexOf(" ms", durationStart);
                entry.Duration = double.Parse(remainingLine.Substring(durationStart, durationEnd - durationStart), CultureInfo.InvariantCulture);
            }

            entry.Parameter = remainingLine.Contains(" ") ? remainingLine.Split(" ")[0] : null;
        }
        catch (Exception)
        {
            return null;
        }

        return entry;
    }
}

class LogEntry
{
    public DateTime DateTime { get; set; }
    public string Action { get; set; }
    public string Parameter { get; set; }
    public string URL { get; set; }
    public string IP { get; set; }
    public double? Duration { get; set; }
}
