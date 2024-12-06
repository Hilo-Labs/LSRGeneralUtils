using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace GenerateCompanyInserts
{
    class Program
    {
        static void Main(string[] args)
        {
            Do not run this by mistake
            //string connectionString = "Data Source=lsr.database.windows.net;Initial Catalog=LSR;Persist Security Info=True;Integrated Security=false;User ID=gateway@lsr;Password=Aiwfcim2ft;MultipleActiveResultSets=True";
            string connectionString = "Data Source=thinker;Initial Catalog=LSRProd;Persist Security Info=True;Integrated Security=false;User ID=hiloUser;Password=Hawaii;MultipleActiveResultSets=True";

            // List of new company names
            List<string> newCompanies = new List<string>
            {
                "A. Cook",
                "A. Death Limited",
                "B.J. Haynes Limited",
                "Baird & Mucklestone Limited",
                "Barber, Wynne, Roberts & Seymour",
                "Blain Martin Surveying Limited",
                "Browne H.J.",
                "Browne, W.A (part)",
                "Carter Horwood Limited",
                "Cook & Dunning",
                "Death, McLean & McMurchy OLS",
                "Frank Barber & Associates",
                "G.S. Abrey",
                "Grant, R.R.",
                "Hwang J.S.",
                "J. McSkimming Limited",
                "James & Wandabense",
                "Lawryshyn, R.B.",
                "McLean, McMurchy & Biason",
                "Ostapiak, D",
                "Robert T. Force OLS",
                "Scott, R.R.",
                "SVN",
                "UME",
                "W.S. Gibson & Sons Limited"
            };

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                foreach (string companyName in newCompanies)
                {
                    // Insert the new company using SELECT from source company (CompanyID = 340)
                    string insertCompanyQuery = $@"
                        INSERT INTO [dbo].[tblCompanies] (
                            [CompanyName], [CompanyNumberPrefix], [roID], [RegOfficeID], [Address],
                            [City], [Province], [Country], [PostalCode], [Phone],
                            [NonAssociatedPercent], [NonAssociated], [PickupLocation], [PickupEmail], [InfoEmail],
                            [fax], [url], [tollfree], [OwnerFlag], [SurveyorFlag],
                            [PublicGlobalFee], [PublicGlobalPercent], [FieldNoteFee], [FieldNotePercent], [PublicGlobalFee2],
                            [PublicGlobalFee3], [PublicGlobalFee4], [MemberPrice1], [MemberPrice2], [MemberPrice3],
                            [MemberPrice4], [PayForOwn], [OnAccount], [MembershipExpiredFlag], [PayingGeoFlag],
                            [RenewalDate], [Warning1Date], [Warning2Date], [PriceUpdated], [AccountingEmail],
                            [StripeUserId], [GtaOwner], [Address2], [ParentCompanyId], [ShareRestrictedFields]
                        )
                        SELECT 
                            '{companyName}', [CompanyNumberPrefix], [roID], [RegOfficeID], [Address],
                            [City], [Province], [Country], [PostalCode], [Phone],
                            [NonAssociatedPercent], [NonAssociated], [PickupLocation], [PickupEmail], [InfoEmail],
                            [fax], [url], [tollfree], [OwnerFlag], [SurveyorFlag],
                            [PublicGlobalFee], [PublicGlobalPercent], [FieldNoteFee], [FieldNotePercent], [PublicGlobalFee2],
                            [PublicGlobalFee3], [PublicGlobalFee4], [MemberPrice1], [MemberPrice2], [MemberPrice3],
                            [MemberPrice4], [PayForOwn], [OnAccount], [MembershipExpiredFlag], [PayingGeoFlag],
                            [RenewalDate], [Warning1Date], [Warning2Date], [PriceUpdated], [AccountingEmail],
                            [StripeUserId], [GtaOwner], [Address2], [ParentCompanyId], [ShareRestrictedFields]
                        FROM [dbo].[tblCompanies]
                        WHERE [CompanyID] = 340;
                        SELECT SCOPE_IDENTITY();";

                    int newCompanyId;
                    using (SqlCommand cmd = new SqlCommand(insertCompanyQuery, conn))
                    {
                        // Execute the insert and retrieve the new CompanyID
                        newCompanyId = Convert.ToInt32(cmd.ExecuteScalar());
                    }

                    Console.WriteLine($"Inserted company: {companyName}, CompanyID: {newCompanyId}");

                    // Add the new company to group 9
                    string insertGroupQuery = $@"
                        INSERT INTO [dbo].[CompanyGroupCompanies] (CompanyGroup_Id, Company_CompanyId)
                        VALUES (9, {newCompanyId});";

                    using (SqlCommand cmd = new SqlCommand(insertGroupQuery, conn))
                    {
                        cmd.ExecuteNonQuery();
                        Console.WriteLine($"Added company {newCompanyId} to group 9.");
                    }
                }
            }

            Console.WriteLine("All companies have been successfully inserted and added to the group.");
        }
    }
}
