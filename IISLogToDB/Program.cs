using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

class Program
{
    const string connectionString = "Data Source=thinker;Initial Catalog=LSRProd;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";
    const string CsvHeader = "Date,Time,SIp,CsMethod,CsUriStem,CsUriQuery,SPort,CsUsername,CIp,CsUserAgent,CsReferer,ScStatus,ScSubstatus,ScWin32Status,TimeTaken";

    static void Main()
    {
        string sourceFolder = @"C:\Users\Alexiel\Desktop\LogFiles";
        string tableName = "tblIISLogsOriginal";

        var logFiles = Directory.GetFiles(sourceFolder, "u_ex*.log").OrderBy(f => f).ToList();

        var groupedFiles = logFiles.GroupBy(f =>
        {
            var fileName = Path.GetFileNameWithoutExtension(f);
            return fileName.Substring(4, 4); // Extract yyMM
        });

        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            //var deleteQuery = $"DELETE FROM {tableName}";
            //using (var deleteCommand = new SqlCommand(deleteQuery, connection))
            //{
            //    try
            //    {
            //        deleteCommand.ExecuteNonQuery();
            //        Console.WriteLine("Table cleared before inserting new data.");
            //    }
            //    catch (Exception ex)
            //    {
            //        Console.WriteLine($"Error deleting data from table: {ex.Message}");
            //    }
            //}

            foreach (var group in groupedFiles)
            {
                Console.WriteLine($"Generating: {group.Key}");

                foreach (var file in group)
                {
                    Console.Write($"  Working with file: {file}");
                    var lines = File.ReadLines(file);

                    foreach (var line in lines)
                    {
                        if (line.StartsWith("#") || string.IsNullOrWhiteSpace(line))
                        {
                            continue;
                        }

                        var columns = line.Split(' ');

                        var query = $"INSERT INTO {tableName} (Date, Time, SIp, CsMethod, CsUriStem, CsUriQuery, SPort, CsUsername, CIp, CsUserAgent, CsReferer, ScStatus, ScSubstatus, ScWin32Status, TimeTaken) " +
                                    "VALUES (@Date, @Time, @SIp, @CsMethod, @CsUriStem, @CsUriQuery, @SPort, @CsUsername, @CIp, @CsUserAgent, @CsReferer, @ScStatus, @ScSubstatus, @ScWin32Status, @TimeTaken)";

                        using (var command = new SqlCommand(query, connection))
                        {
                            command.Parameters.AddWithValue("@Date", columns[0]);
                            command.Parameters.AddWithValue("@Time", columns[1]);
                            command.Parameters.AddWithValue("@SIp", columns[2]);
                            command.Parameters.AddWithValue("@CsMethod", columns[3]);
                            command.Parameters.AddWithValue("@CsUriStem", columns[4]);
                            command.Parameters.AddWithValue("@CsUriQuery", columns[5]);
                            command.Parameters.AddWithValue("@SPort", columns[6]);
                            command.Parameters.AddWithValue("@CsUsername", columns[7]);
                            command.Parameters.AddWithValue("@CIp", columns[8]);
                            command.Parameters.AddWithValue("@CsUserAgent", columns[9]);
                            command.Parameters.AddWithValue("@CsReferer", columns[10]);
                            command.Parameters.AddWithValue("@ScStatus", columns[11]);
                            command.Parameters.AddWithValue("@ScSubstatus", columns[12]);
                            command.Parameters.AddWithValue("@ScWin32Status", columns[13]);
                            command.Parameters.AddWithValue("@TimeTaken", columns[14]);

                            try
                            {
                                command.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Error inserting line: {line}");
                                Console.WriteLine($"Error message: {ex.Message}");
                            }
                        }
                    }
                    Console.WriteLine(" - Done");
                }
            }
        }

        Console.WriteLine("Data inserted into the database successfully.");
    }

    static string EscapeForCsv(string field)
    {
        if (field.Contains(",") || field.Contains("\"") || field.Contains("\n"))
        {
            field = "\"" + field.Replace("\"", "\"\"") + "\"";
        }
        return field;
    }
}
