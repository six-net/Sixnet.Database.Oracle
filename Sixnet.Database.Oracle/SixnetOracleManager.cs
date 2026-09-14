using System.Data;
using Oracle.ManagedDataAccess.Client;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.Oracle
{
    /// <summary>
    /// Defines oracle manager
    /// </summary>
    internal static class SixnetOracleManager
    {
        #region Fields

        /// <summary>
        /// Default query translator
        /// </summary>
        static readonly SixnetOracleDataCommandResolver DefaultResolver = new SixnetOracleDataCommandResolver();

        #endregion

        #region Get database connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        internal static IDbConnection GetConnection(SixnetDatabaseServer server)
        {
            return SixnetDataManager.GetDatabaseConnection(server) ?? new OracleConnection(SixnetDataManager.ResolveConnectionString(server));
        }

        #endregion

        #region Get command resolver

        /// <summary>
        /// Get command resolver
        /// </summary>
        /// <returns>Return a command resolver</returns>
        internal static SixnetOracleDataCommandResolver GetCommandResolver()
        {
            return DefaultResolver;
        }

        #endregion
    }
}
