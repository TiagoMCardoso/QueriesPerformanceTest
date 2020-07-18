using System.Configuration;

namespace TMC.QueriesPerformanceTest
{
    public class SqlForCdsAuthHelper
    {
        public string SQLQuery;
        public string SQLConnectionString;

        public SqlForCdsAuthHelper()
        {
            SQLQuery = ConfigurationManager.AppSettings["SQLQuery"];
            SQLConnectionString = ConfigurationManager.AppSettings["SQLConnectionString"];
        }
    }
}