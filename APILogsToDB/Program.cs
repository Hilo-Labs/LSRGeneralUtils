using System;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;

class Program
{
    const string connectionString = "Data Source=thinker;Initial Catalog=LSRProd;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";

    static void Main()
    {
        string logFilePath = @"C:\Development\Utils\APILogsToDB\Logs\logs.txt";
        string tableName = "LogEntries";

        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            var lines = File.ReadAllLines(logFilePath);

            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (!line.StartsWith("[")) continue;

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
            var logLevel = timestampParts[2];

            var utcDateTime = DateTime.ParseExact(timestampString, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            entry.DateTime = TimeZoneInfo.ConvertTimeFromUtc(utcDateTime, TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"));

            var actionStart = timestampEnd + 2;
            var actionEnd = line.IndexOf(":", actionStart);

            string actionText;
            if (actionEnd > 0) // Check if ':' exists in the line
            {
                actionText = line.Substring(actionStart, actionEnd - actionStart).Trim();
            }
            else
            {
                actionText = line.Substring(actionStart).Trim();
            }
            entry.Action = $"{logLevel}: {actionText}";

            switch (entry.Action)
            {
                case var action when action.Contains(": Throttling "):
                    {
                        if (line.Contains("seconds:"))
                        {
                            var secondsIndex = line.LastIndexOf("seconds:") + 9; 
                            var parameterValue = line.Substring(secondsIndex).Trim();
                            entry.Parameter = parameterValue;
                        }

                        if (line.Contains("IP:"))
                        {
                            var ipStart = line.IndexOf("IP:") + 4; 
                            var ipEnd = line.IndexOf(". ", ipStart); 
                            entry.IP = ipEnd > ipStart ? line.Substring(ipStart, ipEnd - ipStart).Trim() : null;
                        }
                        break;
                    }
                case var action when
                    action.StartsWith("Throttling applied") ||
                    action.StartsWith("Throttling applied"):
                default:
                    {
                        var remainingLine = line.Substring(actionEnd + 1).Trim();
                        var parameterEnd = remainingLine.IndexOf("\"");
                        entry.Parameter = parameterEnd > 0 ? remainingLine.Substring(0, parameterEnd).Trim() : null;

                        if (remainingLine.Contains("\""))
                        {
                            var urlStart = remainingLine.IndexOf("\"") + 1;
                            var urlEnd = remainingLine.IndexOf("\"", urlStart);
                            entry.URL = urlEnd > urlStart ? remainingLine.Substring(urlStart, urlEnd - urlStart) : null;

                            remainingLine = remainingLine.Substring(urlEnd + 1);
                        }

                        if (remainingLine.Contains(" from "))
                        {
                            var ipStart = remainingLine.IndexOf(" from ") + 6;
                            var ipEnd = remainingLine.IndexOf(" ", ipStart);
                            entry.IP = ipEnd > ipStart ? remainingLine.Substring(ipStart, ipEnd - ipStart) : remainingLine.Substring(ipStart);
                        }

                        if (remainingLine.Contains("Duration:"))
                        {
                            var durationStart = remainingLine.IndexOf("Duration:") + 9;
                            var durationEnd = remainingLine.IndexOf(" ms", durationStart);
                            if (durationEnd > durationStart)
                            {
                                entry.Duration = double.Parse(remainingLine.Substring(durationStart, durationEnd - durationStart), CultureInfo.InvariantCulture);
                            }
                        }
                        break;
                    }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
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
