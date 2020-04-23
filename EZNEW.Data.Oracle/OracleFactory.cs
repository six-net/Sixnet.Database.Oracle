using EZNEW.Data.Config;
using EZNEW.Data.CriteriaConvert;
using EZNEW.Develop.CQuery.CriteriaConvert;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Framework.Fault;
using Oracle.ManagedDataAccess.Client;
using System.Data;

namespace EZNEW.Data.Oracle
{
    /// <summary>
    /// db server factory
    /// </summary>
    internal static class OracleFactory
    {
        #region get db connection

        /// <summary>
        /// get mysql database connection
        /// </summary>
        /// <param name="server">database server</param>
        /// <returns>db connection</returns>
        public static IDbConnection GetConnection(ServerInfo server)
        {
            IDbConnection conn = DataManager.GetDBConnection?.Invoke(server) ?? new OracleConnection(server.ConnectionString);
            return conn;
        }

        #endregion

        #region get query translator

        /// <summary>
        /// get query translator
        /// </summary>
        /// <param name="server">database server</param>
        /// <returns></returns>
        internal static IQueryTranslator GetQueryTranslator(ServerInfo server)
        {
            var translator = DataManager.GetQueryTranslator?.Invoke(server);
            if (translator == null)
            {
                translator = new OracleQueryTranslator();
            }
            return translator;
        }

        #endregion

        #region criteria convert

        /// <summary>
        /// parse criteria convert
        /// </summary>
        /// <param name="convert">convert</param>
        /// <param name="objectName">object name</param>
        /// <param name="fieldName">field name</param>
        /// <returns></returns>
        internal static string ParseCriteriaConvert(ICriteriaConvert convert, string objectName, string fieldName)
        {
            var criteriaConvertParse = DataManager.GetCriteriaConvertParse(convert?.Name) ?? Parse;
            return criteriaConvertParse(new CriteriaConvertParseOption()
            {
                CriteriaConvert = convert,
                ServerType = ServerType.Oracle,
                ObjectName = objectName,
                FieldName = fieldName
            });
        }

        /// <summary>
        /// parse
        /// </summary>
        /// <param name="option">parse option</param>
        /// <returns></returns>
        static string Parse(CriteriaConvertParseOption option)
        {
            if (string.IsNullOrWhiteSpace(option?.CriteriaConvert?.Name))
            {
                throw new EZNEWException("criteria convert config name is null or empty");
            }
            string format = null;
            switch (option.CriteriaConvert.Name)
            {
                case CriteriaConvertNames.StringLength:
                    format = $"LENGTH({option.ObjectName}.{option.FieldName})";
                    break;
            }
            if (string.IsNullOrWhiteSpace(format))
            {
                throw new EZNEWException($"cann't resolve criteria convert:{option.CriteriaConvert.Name} for Oracle");
            }
            return format;
        }

        #endregion
    }
}
