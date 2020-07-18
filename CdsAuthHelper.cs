using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System;
using System.Configuration;

namespace TMC.QueriesPerformanceTest
{
    public class CdsAuthHelper
    {
        public string token = string.Empty;
        public Uri serviceUrl = null;

        private string organizationSvc;
        private string organizationUrl;
        private string clientAppId;
        private string clientSecretId;
        private string tenantId;
        private string authority;

        public CdsAuthHelper()
        {
            organizationSvc = ConfigurationManager.AppSettings["organizationSvc"];
            organizationUrl = ConfigurationManager.AppSettings["organizationUrl"];
            clientAppId = ConfigurationManager.AppSettings["clientAppId"];
            clientSecretId = ConfigurationManager.AppSettings["clientSecretId"];
            tenantId = ConfigurationManager.AppSettings["tenantId"];
            authority = ConfigurationManager.AppSettings["authority"];

            var authContext = new AuthenticationContext($"{authority}{tenantId}");
            var credential = new ClientCredential(clientAppId, clientSecretId);
            var result = authContext.AcquireTokenAsync(organizationUrl, credential).Result;

            // Set CDS Service URL
            serviceUrl = new Uri($"{organizationUrl}{organizationSvc}");

            // Get Authenticated Token
            token = result.AccessToken;

        }
    }
}

