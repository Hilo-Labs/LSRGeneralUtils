using System;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Dapper;

namespace ReportGenerator
{
    public class Program
    {
        static async Task<int> Main()
        {
            string dbConnectionString = "Data Source=thinker;Initial Catalog=LSRProd;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";
            string outputFilePath = @"C:\Development\Utils\LegacyUsageReport\Usage.txt";
            var ignoreLinks = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "/menu.asp",
                "/donebutton.asp",
                "/imagebuttonsowner.asp"
            };

            // Define minimum date
            DateTime minDate = new DateTime(2023, 1, 1);

            // List of IP addresses to exclude
            var excludeIPs = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "99.248.34.40"
            };

            // List of Companies to exclude
            var excludeCompanies = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "Gateway Software Productions"
            };

            try
            {
                using (IDbConnection db = new SqlConnection(dbConnectionString))
                {
                    // Get mapping of IP addresses to CompanyIDs
                    var ipToCompany = await db.QueryAsync<(string IP, int CompanyID)>(@"
                        SELECT DISTINCT nl.IP, u.CompanyID
                        FROM tblNavigationLogs nl
                        JOIN tblUsers u ON nl.UserID = u.UserID
                        WHERE u.CompanyID IS NOT NULL AND nl.IP IS NOT NULL
                    ");

                    // Build dictionary of IP to list of CompanyIDs
                    var ipToCompaniesDict = new Dictionary<string, List<int>>();
                    foreach (var item in ipToCompany)
                    {
                        if (!ipToCompaniesDict.ContainsKey(item.IP))
                            ipToCompaniesDict[item.IP] = new List<int>();
                        if (!ipToCompaniesDict[item.IP].Contains(item.CompanyID))
                            ipToCompaniesDict[item.IP].Add(item.CompanyID);
                    }

                    // Get mapping of CompanyID to CompanyName
                    var companies = await db.QueryAsync<(int CompanyID, string CompanyName)>(@"
                        SELECT companyID AS CompanyID, CompanyName FROM tblCompanies
                    ");
                    var companyNames = companies.ToDictionary(x => x.CompanyID, x => x.CompanyName);

                    // Get company group mappings
                    var companyGroups = await db.QueryAsync<(int CompanyID, string GroupName)>(@"
                        SELECT cgc.Company_CompanyId AS CompanyID, cg.Description AS GroupName
                        FROM CompanyGroupCompanies cgc
                        JOIN tblCompanyGroups cg ON cgc.CompanyGroup_Id = cg.Id
                    ");
                    var companyGroupDict = companyGroups.ToDictionary(x => x.CompanyID, x => x.GroupName);

                    // Function to get display name (group name or company name)
                    string GetDisplayName(int companyId)
                    {
                        if (companyGroupDict.TryGetValue(companyId, out string groupName))
                        {
                            return groupName;
                        }
                        else if (companyNames.TryGetValue(companyId, out string companyName))
                        {
                            return companyName;
                        }
                        else
                        {
                            return "Unknown Company";
                        }
                    }

                    // Initialize logs dictionary
                    Dictionary<string, List<LogEntry>> logsByDisplayName = new Dictionary<string, List<LogEntry>>();
                    Dictionary<string, List<LogEntry>> otherLogsByIP = new Dictionary<string, List<LogEntry>>();

                    // Get all entries from tblIISLogs with Date >= minDate
                    var iisLogs = await db.QueryAsync<LogEntry>(@"
                        SELECT [Date], [Time], [CIp], [CsUriStem], [CsUriQuery], [ScStatus]
                        FROM tblIISLogs
                        WHERE [Date] >= @MinDate
                    ", new { MinDate = minDate });

                    foreach (var log in iisLogs)
                    {
                        if (ignoreLinks.Contains(log.CsUriStem))
                            continue;

                        if (excludeIPs.Contains(log.CIp))
                            continue;

                        if (ipToCompaniesDict.TryGetValue(log.CIp, out List<int> companyIds))
                        {
                            foreach (var companyId in companyIds)
                            {
                                string displayName = GetDisplayName(companyId);

                                if (excludeCompanies.Contains(displayName))
                                    continue;

                                if (!logsByDisplayName.ContainsKey(displayName))
                                    logsByDisplayName[displayName] = new List<LogEntry>();
                                logsByDisplayName[displayName].Add(log);
                            }
                        }
                        else
                        {
                            if (!otherLogsByIP.ContainsKey(log.CIp))
                                otherLogsByIP[log.CIp] = new List<LogEntry>();
                            otherLogsByIP[log.CIp].Add(log);
                        }
                    }

                    using (var writer = new StreamWriter(outputFilePath))
                    {
                        // Generate report for known companies/groups
                        foreach (var kvp in logsByDisplayName)
                        {
                            string displayName = kvp.Key;
                            List<LogEntry> logs = kvp.Value;

                            writer.WriteLine($"Company/Group: {displayName}");

                            // Group logs by CsUriStem
                            var pageAccesses = logs.GroupBy(l => l.CsUriStem)
                                                   .Select(g => new { Page = g.Key, Count = g.Count() })
                                                   .OrderByDescending(x => x.Count);

                            foreach (var page in pageAccesses)
                            {
                                writer.WriteLine($"    {page.Page}: {page.Count} hits");
                            }

                            writer.WriteLine();
                        }

                        // Generate report for unknown IPs, sorted by total hits
                        var sortedUnknownIPs = otherLogsByIP
                            .Where(kvp => !excludeIPs.Contains(kvp.Key))
                            .OrderByDescending(kvp => kvp.Value.Count);

                        writer.WriteLine();
                        writer.WriteLine("==========================================================");
                        writer.WriteLine();

                        foreach (var kvp in sortedUnknownIPs)
                        {
                            string ip = kvp.Key;
                            List<LogEntry> logs = kvp.Value;

                            writer.WriteLine($"Unknown IP: {ip} ({logs.Count} hits)");

                            // Group logs by CsUriStem
                            var pageAccesses = logs.GroupBy(l => l.CsUriStem)
                                                   .Select(g => new { Page = g.Key, Count = g.Count() })
                                                   .OrderByDescending(x => x.Count);

                            foreach (var page in pageAccesses)
                            {
                                writer.WriteLine($"    {page.Page}: {page.Count} hits");
                            }

                            writer.WriteLine();
                        }
                    }

                    return 0;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred during processing:");
                Console.WriteLine(ex.Message);
                return 1;
            }
        }

        public class LogEntry
        {
            public DateTime Date { get; set; }
            public TimeSpan Time { get; set; }
            public string CIp { get; set; }
            public string CsUriStem { get; set; }
            public string CsUriQuery { get; set; }
            public int ScStatus { get; set; }
        }
    }
}
