using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;

namespace EmailRecovery
{
    public class Program
    {
        static async Task<int> Main()
        {
            Dont run this by mistake

            string primaryDbConnectionString = "";// Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=web_app_login@lsr;Password=IUOPYjhf(*2390FOIJHjndjSsuwqp!!oI{MNS7yds7;MultipleActiveResultSets=True";
            string secondaryDbConnectionString = "";// Data Source=thinker;Initial Catalog=20241113 - LSRProd_Backup;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";

            var companyEmails = await RetrieveCompanyEmailsFromSecondaryDb(secondaryDbConnectionString);
            var userEmails = await RetrieveUserEmailsFromSecondaryDb(secondaryDbConnectionString);
            var aspNetUserEmails = await RetrieveAspNetUserEmailsFromSecondaryDb(secondaryDbConnectionString);
            var transactionEmails = await RetrieveTransactionEmailsFromSecondaryDb(secondaryDbConnectionString);

            await UpdatePrimaryDbWithCompanyEmails(primaryDbConnectionString, companyEmails);
            await UpdatePrimaryDbWithUserEmails(primaryDbConnectionString, userEmails);
            await UpdatePrimaryDbWithAspNetUserEmails(primaryDbConnectionString, aspNetUserEmails);
            await UpdatePrimaryDbWithTransactionEmails(primaryDbConnectionString, transactionEmails);

            return 0;
        }

        private static async Task<IEnumerable<CompanyEmail>> RetrieveCompanyEmailsFromSecondaryDb(string connectionString)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                return await db.QueryAsync<CompanyEmail>("SELECT CompanyID, InfoEmail, AccountingEmail, PickupEmail FROM tblCompanies");
            }
        }

        private static async Task<IEnumerable<UserEmail>> RetrieveUserEmailsFromSecondaryDb(string connectionString)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                return await db.QueryAsync<UserEmail>("SELECT UserID, Email FROM tblUsers");
            }
        }

        private static async Task<IEnumerable<AspNetUserEmail>> RetrieveAspNetUserEmailsFromSecondaryDb(string connectionString)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                return await db.QueryAsync<AspNetUserEmail>("SELECT LegacyUser_Id, Email FROM AspNetUsers");
            }
        }

        private static async Task<IEnumerable<TransactionEmail>> RetrieveTransactionEmailsFromSecondaryDb(string connectionString)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                return await db.QueryAsync<TransactionEmail>("SELECT TransactionID, Email FROM tblTransactions");
            }
        }

        private static async Task UpdatePrimaryDbWithCompanyEmails(string connectionString, IEnumerable<CompanyEmail> companyEmails)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                foreach (var email in companyEmails)
                {
                    await db.ExecuteAsync(
                        "UPDATE tblCompanies SET InfoEmail = @InfoEmail, AccountingEmail = @AccountingEmail, PickupEmail = @PickupEmail WHERE CompanyID = @CompanyID",
                        email);
                }
            }
        }

        private static async Task UpdatePrimaryDbWithUserEmails(string connectionString, IEnumerable<UserEmail> userEmails)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                foreach (var email in userEmails)
                {
                    await db.ExecuteAsync(
                        "UPDATE tblUsers SET Email = @Email WHERE UserID = @UserID",
                        email);
                }
            }
        }

        private static async Task UpdatePrimaryDbWithAspNetUserEmails(string connectionString, IEnumerable<AspNetUserEmail> aspNetUserEmails)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                foreach (var email in aspNetUserEmails)
                {
                    await db.ExecuteAsync(
                        "UPDATE AspNetUsers SET Email = @Email WHERE LegacyUser_Id = @LegacyUser_Id",
                        email);
                }
            }
        }

        private static async Task UpdatePrimaryDbWithTransactionEmails(string connectionString, IEnumerable<TransactionEmail> transactionEmails)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                foreach (var email in transactionEmails)
                {
                    await db.ExecuteAsync(
                        "UPDATE tblTransactions SET Email = @Email WHERE TransactionID = @TransactionID",
                        email);
                }
            }
        }
    }

    public class CompanyEmail
    {
        public int CompanyID { get; set; }
        public string InfoEmail { get; set; }
        public string AccountingEmail { get; set; }
        public string PickupEmail { get; set; }
    }

    public class UserEmail
    {
        public int UserID { get; set; }
        public string Email { get; set; }
    }

    public class AspNetUserEmail
    {
        public int LegacyUser_Id { get; set; }
        public string Email { get; set; }
    }

    public class TransactionEmail
    {
        public int TransactionID { get; set; }
        public string Email { get; set; }
    }
}
