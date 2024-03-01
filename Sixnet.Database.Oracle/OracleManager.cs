using System.Data;
using Oracle.ManagedDataAccess.Client;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.Oracle
{
    /// <summary>
    /// Defines oracle manager
    /// </summary>
    internal static class OracleManager
    {
        #region Fields

        /// <summary>
        /// Gets current database server type
        /// </summary>
        internal const DatabaseServerType CurrentDatabaseServerType = DatabaseServerType.Oracle;

        /// <summary>
        /// Key word prefix
        /// </summary>
        internal const string KeywordPrefix = "\"";

        /// <summary>
        /// Key word suffix
        /// </summary>
        internal const string KeywordSuffix = "\"";

        /// <summary>
        /// Oracle options
        /// </summary>
        internal static readonly OracleOptions OracleOptions = new OracleOptions();

        /// <summary>
        /// Default query translator
        /// </summary>
        static readonly OracleDataCommandResolver DefaultResolver = new OracleDataCommandResolver();

        #endregion

        #region Get database connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        internal static IDbConnection GetConnection(SixnetDatabaseServer server)
        {
            return SixnetDataManager.GetDatabaseConnection(server) ?? new OracleConnection(server.ConnectionString);
        }

        #endregion

        #region Get command resolver

        /// <summary>
        /// Get command resolver
        /// </summary>
        /// <returns>Return a command resolver</returns>
        internal static OracleDataCommandResolver GetCommandResolver()
        {
            return DefaultResolver;
        }

        #endregion

        #region Format keyword

        internal static string FormatKeyword(string originalValue)
        {
            if (OracleOptions.Uppercase)
            {
                originalValue = originalValue.ToUpper();
            }
            if (OracleOptions.WrapWithQuotes)
            {
                originalValue = $"{WrapKeyword(originalValue)}";
            }
            return originalValue;
        }

        #endregion

        #region Wrap keyword

        /// <summary>
        /// Wrap keyword by the KeywordPrefix and the KeywordSuffix
        /// </summary>
        /// <param name="originalValue">Original value</param>
        /// <returns></returns>
        internal static string WrapKeyword(string originalValue)
        {
            return $"{KeywordPrefix}{originalValue}{KeywordSuffix}";
        }

        #endregion
    }
}
