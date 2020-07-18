using Microsoft.Pfe.Xrm;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.WebServiceClient;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using TMC.D365.Entities;

namespace TMC.QueriesPerformanceTest
{
    public class Program
    {
        private const string queryResultsFileName = "query_results";
        private const int numberOfTests = 100;
        private const int recordsPerPage = 5000;
        private static int count;
        private static TimeSpan diff;
        
        static void Main(string[] args)
        {
            StringBuilder log = new StringBuilder();
            DateTime start;
            string line;

            #region Execute Queries
            //SQLForCDS
            for (int i = 0; i < numberOfTests; i++)
            {
                log = new StringBuilder();
                start = DateTime.Now;

                SQLForCDS();

                diff = DateTime.Now - start;

                line = $"SQLForCDS|Total Time: {diff}|Total records retrieved: {count}";

                log.AppendLine(line);

                Console.WriteLine(line);

                Log(queryResultsFileName, log);
            }

            //FetchParallel
            for (int i = 0; i < numberOfTests; i++)
            {
                log = new StringBuilder();
                start = DateTime.Now;

                FetchParallel();

                diff = DateTime.Now - start;

                line = $"FetchParallel|Total Time: {diff}|Total records retrieved: {count}";

                log.AppendLine(line);

                Console.WriteLine(line);

                Log(queryResultsFileName, log);
            }

            //LinqEarlyBound
            for (int i = 0; i < numberOfTests; i++)
            {
                log = new StringBuilder();
                start = DateTime.Now;

                LinqEarlyBound();

                diff = DateTime.Now - start;

                line = $"LinqEarlyBound|Total Time: {diff}|Total records retrieved: {count}";

                log.AppendLine(line);

                Console.WriteLine(line);

                Log(queryResultsFileName, log);
            }

            //LinqLateBound
            for (int i = 0; i < numberOfTests; i++)
            {
                log = new StringBuilder();
                start = DateTime.Now;

                LinqLateBound();

                diff = DateTime.Now - start;

                line = $"LinqLateBound|Total Time: {diff}|Total records retrieved: {count}";

                log.AppendLine(line);

                Console.WriteLine(line);

                Log(queryResultsFileName, log);
            }

            //QueryExpression
            for (int i = 0; i < numberOfTests; i++)
            {
                log = new StringBuilder();
                start = DateTime.Now;

                QueryExpression();

                diff = DateTime.Now - start;

                line = $"QueryExpression|Total Time: {diff}|Total records retrieved: {count}";

                log.AppendLine(line);

                Console.WriteLine(line);

                Log(queryResultsFileName, log);
            }

            //FetchXml
            for (int i = 0; i < numberOfTests; i++)
            {
                log = new StringBuilder();
                start = DateTime.Now;

                FetchXml();

                diff = DateTime.Now - start;

                line = $"FetchXml|Total Time: {diff}|Total records retrieved: {count}";

                log.AppendLine(line);

                Console.WriteLine(line);

                Log(queryResultsFileName, log);
            }
            #endregion
        }

        private static void FetchXml()
        {
            count = 0;
            CdsAuthHelper cdsAuthHelper = new CdsAuthHelper();

            using (var proxy = new OrganizationWebProxyClient(cdsAuthHelper.serviceUrl, false))
            {
                // Set Header with the token
                proxy.HeaderToken = cdsAuthHelper.token;

                // Get Organization Service
                IOrganizationService service = proxy;
                               
                string fetch = "<fetch mapping='logical' no-lock='true'>";
                fetch += "<entity name='account'>";
                fetch += "<attribute name='name'/>";
                fetch += "<attribute name='accountid'/>";
                fetch += "</entity>";
                fetch += "</fetch>";

                // Initialize the page number.
                int pageNumber = 1;

                // Specify the current paging cookie. For retrieving the first page, pagingCookie should be null.
                string pagingCookie = null;

                while (true)
                {
                    // Build fetchXml string with the placeholders.
                    string xml = CreateXml(fetch, pagingCookie, pageNumber, recordsPerPage);

                    FetchExpression expression = new FetchExpression(xml);
                    var results = service.RetrieveMultiple(expression);

                    // Check for morerecords, if it returns 1.
                    if (results.MoreRecords)
                    {
                        // Increment the page number to retrieve the next page.
                        pageNumber++;
                        pagingCookie = results.PagingCookie;
                    }
                    else
                    {
                        // If no more records in the result nodes, exit the loop.
                        count = (recordsPerPage * pageNumber) + results.Entities.Count;

                        break;
                    }
                }
            }
        }

        private static void LinqEarlyBound()
        {
            count = 0;
            CdsAuthHelper cdsAuthHelper = new CdsAuthHelper();

            using (var proxy = new OrganizationWebProxyClient(cdsAuthHelper.serviceUrl, true))
            {
                // Set Header with the token
                proxy.HeaderToken = cdsAuthHelper.token;

                // Get Crm Service Context (Early bound)
                CrmServiceContext context = new CrmServiceContext(proxy);

                // Retrieve Account records
                var accounts = (from m in context.AccountSet
                                orderby m.AccountId
                                select new
                                {
                                    m.Name,
                                    m.AccountId
                                }
                                );

                count = accounts.ToList().Count;
            }
        }

        private static void LinqLateBound()
        {
            count = 0;
            CdsAuthHelper cdsAuthHelper = new CdsAuthHelper();

            using (var proxy = new OrganizationWebProxyClient(cdsAuthHelper.serviceUrl, false))
            {
                // Set Header with the token
                proxy.HeaderToken = cdsAuthHelper.token;

                // Get Organization Service Context (Late bound)
                OrganizationServiceContext context = new OrganizationServiceContext(proxy);

                // Retrieve Account records
                var accounts = (from m in context.CreateQuery("account")
                                orderby m.GetAttributeValue<Guid>("accountid")
                                select new
                                {
                                    name = m.GetAttributeValue<string>("name"),
                                    accountid = m.GetAttributeValue<Guid>("accountid"),
                                }
                                );

                count = accounts.ToList().Count;
            }
        }

        private static void QueryExpression()
        {
            count = 0;
            CdsAuthHelper cdsAuthHelper = new CdsAuthHelper();

            using (var proxy = new OrganizationWebProxyClient(cdsAuthHelper.serviceUrl, false))
            {
                // Set Header with the token
                proxy.HeaderToken = cdsAuthHelper.token;

                // Get Organization Service
                IOrganizationService service = proxy;

                // Initialize the page number.
                int pageNumber = 1;

                //Create a column set.
                ColumnSet columns = new ColumnSet("name", "accountid");

                // Create query expression.
                QueryExpression query = new QueryExpression();
                query.ColumnSet = columns;
                query.EntityName = "account";
                query.NoLock = true;

                // Assign the pageinfo properties to the query expression.
                query.PageInfo = new PagingInfo();
                query.PageInfo.Count = recordsPerPage;
                query.PageInfo.PageNumber = pageNumber;
                query.NoLock = true;

                // The current paging cookie. When retrieving the first page, pagingCookie should be null.
                query.PageInfo.PagingCookie = null;

                while (true)
                {
                    // Retrieve the page.
                    EntityCollection results = service.RetrieveMultiple(query);

                    // Check for more records, if it returns true.
                    if (results.MoreRecords)
                    {
                        pageNumber++;

                        // Increment the page number to retrieve the next page.
                        query.PageInfo.PageNumber = pageNumber;

                        // Set the paging cookie to the paging cookie returned from current results.
                        query.PageInfo.PagingCookie = results.PagingCookie;
                    }
                    else
                    {
                        // If no more records are in the result nodes, exit the loop.
                        count = (recordsPerPage * pageNumber) + results.Entities.Count;
                        break;
                    }
                }
            }
        }

        private static void FetchParallel()
        {
            count = 0;
            CdsAuthHelper cdsAuthHelper = new CdsAuthHelper();

            using (var proxy = new OrganizationWebProxyClient(cdsAuthHelper.serviceUrl, false))
            {
                // Set Header with the token
                proxy.HeaderToken = cdsAuthHelper.token;

                string fetch = "<fetch mapping='logical' no-lock='true'>";
                fetch += "<entity name='account'>";
                fetch += "<attribute name='name'/>";
                fetch += "<attribute name='accountid'/>";
                fetch += "</entity>";
                fetch += "</fetch>";

                IDictionary<string, QueryBase> queries = new Dictionary<string, QueryBase>();
                queries.Add("result", new FetchExpression(fetch));

                Microsoft.Xrm.Tooling.Connector.CrmServiceClient context = new Microsoft.Xrm.Tooling.Connector.CrmServiceClient(proxy);

                var manager = new OrganizationServiceManager(context);

                IDictionary<string, EntityCollection> results = null;

                try
                {
                    results = manager.ParallelProxy.RetrieveMultiple(queries, true);

                    foreach (var result in results)
                    {
                        count += result.Value.Entities.Count;
                    }
                }
                catch (AggregateException ae)
                {
                    // Handle exceptions
                }
            }
        }

        private static void SQLForCDS()
        {
            count = 0;

            SqlForCdsAuthHelper sqlForCDSAuthHelper = new SqlForCdsAuthHelper();

            using (SqlConnection connection = new SqlConnection(sqlForCDSAuthHelper.SQLConnectionString))
            {
                SqlCommand command = new SqlCommand(sqlForCDSAuthHelper.SQLQuery, connection);
                connection.Open();

                SqlDataReader reader = command.ExecuteReader();

                try
                {
                    while (reader.Read())
                    {
                        count++;
                    }
                }
                catch (Exception ex)
                {
                    //log.Info("Can not open connection ! ");
                }
                finally
                {
                    // Always call Close when done reading.
                    reader.Close();
                }
            }
        }

        private static string CreateXml(string xml, string cookie, int page, int count)
        {
            StringReader stringReader = new StringReader(xml);
            XmlTextReader reader = new XmlTextReader(stringReader);

            // Load document
            XmlDocument doc = new XmlDocument();
            doc.Load(reader);

            return CreateXml(doc, cookie, page, count);
        }

        private static string CreateXml(XmlDocument doc, string cookie, int page, int count)
        {
            XmlAttributeCollection attrs = doc.DocumentElement.Attributes;

            if (cookie != null)
            {
                XmlAttribute pagingAttr = doc.CreateAttribute("paging-cookie");
                pagingAttr.Value = cookie;
                attrs.Append(pagingAttr);
            }

            XmlAttribute pageAttr = doc.CreateAttribute("page");
            pageAttr.Value = System.Convert.ToString(page);
            attrs.Append(pageAttr);

            XmlAttribute countAttr = doc.CreateAttribute("count");
            countAttr.Value = System.Convert.ToString(count);
            attrs.Append(countAttr);

            StringBuilder sb = new StringBuilder(1024);
            StringWriter stringWriter = new StringWriter(sb);

            XmlTextWriter writer = new XmlTextWriter(stringWriter);
            doc.WriteTo(writer);
            writer.Close();

            return sb.ToString();
        }

        private static void Log(string querytype, StringBuilder text)
        {
            string path = @"C:\SOME_FOLDER\" + querytype + ".txt";
            using (TextWriter tw = new StreamWriter(path, true))
            {
                // Add some information to the file.
                tw.Write(text.ToString());
            }
        }
    }
}