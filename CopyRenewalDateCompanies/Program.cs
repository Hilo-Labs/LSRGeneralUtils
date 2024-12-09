using System;
using System.Data;
using System.Data.SqlClient;

namespace CopyRenewalDateCompanies
{
    class Program
    {
        static void Main(string[] args)
        {
            string sourceConnectionString = "Data Source=thinker;Initial Catalog=20241113 - LSRProd_Backup;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";
            string targetConnectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";

            using (SqlConnection sourceConn = new SqlConnection(sourceConnectionString))
            using (SqlConnection targetConn = new SqlConnection(targetConnectionString))
            {
                sourceConn.Open();
                targetConn.Open();

                // Retrieve CompanyIDs from the target database where RenewalDate is NULL
                string getTargetIdsQuery = @"
                    SELECT companyID 
                    FROM [dbo].[tblCompanies]
                    WHERE 1=1
                    AND RenewalDate IS NULL
                ";

                SqlCommand targetCommand = new SqlCommand(getTargetIdsQuery, targetConn);
                SqlDataReader targetReader = targetCommand.ExecuteReader();

                // Iterate through the target CompanyIDs
                while (targetReader.Read())
                {
                    int targetCompanyId = targetReader.GetInt32(0);

                    // Retrieve data from the source database for the current CompanyID
                    string getSourceDataQuery = @"
                        SELECT PickupLocation, PickupEmail, fax, url, tollfree, RenewalDate, Warning1Date, Warning2Date, AccountingEmail
                        FROM [dbo].[tblCompanies]
                        WHERE companyID = @CompanyID";

                    SqlCommand sourceCommand = new SqlCommand(getSourceDataQuery, sourceConn);
                    sourceCommand.Parameters.AddWithValue("@CompanyID", targetCompanyId);

                    using (SqlDataReader sourceReader = sourceCommand.ExecuteReader())
                    {
                        if (sourceReader.Read())
                        {
                            // Prepare to update the target database
                            string updateTargetQuery = @"
                                UPDATE [dbo].[tblCompanies]
                                SET 
                                    PickupLocation = @PickupLocation,
                                    PickupEmail = @PickupEmail,
                                    fax = @Fax,
                                    url = @Url,
                                    tollfree = @TollFree,
                                    RenewalDate = @RenewalDate,
                                    Warning1Date = @Warning1Date,
                                    Warning2Date = @Warning2Date,
                                    AccountingEmail = @AccountingEmail
                                WHERE companyID = @CompanyID";

                            SqlCommand updateCommand = new SqlCommand(updateTargetQuery, targetConn);

                            // Map values from the source to the target
                            updateCommand.Parameters.AddWithValue("@PickupLocation", sourceReader["PickupLocation"] ?? DBNull.Value);
                            updateCommand.Parameters.AddWithValue("@PickupEmail", sourceReader["PickupEmail"] ?? DBNull.Value);
                            updateCommand.Parameters.AddWithValue("@Fax", sourceReader["fax"] ?? DBNull.Value);
                            updateCommand.Parameters.AddWithValue("@Url", sourceReader["url"] ?? DBNull.Value);
                            updateCommand.Parameters.AddWithValue("@TollFree", sourceReader["tollfree"] ?? DBNull.Value);
                            updateCommand.Parameters.AddWithValue("@RenewalDate", sourceReader["RenewalDate"] ?? DBNull.Value);
                            updateCommand.Parameters.AddWithValue("@Warning1Date", sourceReader["Warning1Date"] ?? DBNull.Value);
                            updateCommand.Parameters.AddWithValue("@Warning2Date", sourceReader["Warning2Date"] ?? DBNull.Value);
                            updateCommand.Parameters.AddWithValue("@AccountingEmail", sourceReader["AccountingEmail"] ?? DBNull.Value);
                            updateCommand.Parameters.AddWithValue("@CompanyID", targetCompanyId);

                            // Execute the update
                            int rowsAffected = updateCommand.ExecuteNonQuery();
                            Console.WriteLine($"Updated CompanyID {targetCompanyId} with {rowsAffected} row(s) affected.");
                        }
                    }
                }
            }

            Console.WriteLine("Data synchronization complete.");
        }
    }
}
