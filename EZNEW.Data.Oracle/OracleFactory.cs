using System;
using System.Data;
using System.Linq;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using EZNEW.Data.CriteriaConverter;
using EZNEW.Development.Query.CriteriaConverter;
using EZNEW.Development.Query.Translator;
using EZNEW.Exceptions;
using EZNEW.Logging;
using EZNEW.Serialization;
using EZNEW.Development.Query;
using EZNEW.Development.Command;
using EZNEW.Development.Entity;
using EZNEW.Dapper;
using EZNEW.Development.DataAccess;
using EZNEW.Development.Command.Modification;
using EZNEW.Diagnostics;

namespace EZNEW.Data.Oracle
{
    /// <summary>
    /// Database server factory
    /// </summary>
    public static class OracleFactory
    {
        #region Fields

        /// <summary>
        /// Field format key
        /// </summary>
        internal static readonly string fieldFormatKey = ((int)DatabaseServerType.Oracle).ToString();

        /// <summary>
        /// Parameter prefix
        /// </summary>
        internal const string parameterPrefix = ":";

        /// <summary>
        /// Key word prefix
        /// </summary>
        internal const string KeywordPrefix = "\"";

        /// <summary>
        /// Key word suffix
        /// </summary>
        internal const string KeywordSuffix = "\"";

        /// <summary>
        /// Calculate operators
        /// </summary>
        static readonly Dictionary<CalculationOperator, string> CalculateOperators = new Dictionary<CalculationOperator, string>(4)
        {
            [CalculationOperator.Add] = "+",
            [CalculationOperator.Subtract] = "-",
            [CalculationOperator.Multiply] = "*",
            [CalculationOperator.Divide] = "/",
        };

        /// <summary>
        /// Aggregate functions
        /// </summary>
        static readonly Dictionary<CommandOperationType, string> AggregateFunctions = new Dictionary<CommandOperationType, string>(5)
        {
            [CommandOperationType.Max] = "MAX",
            [CommandOperationType.Min] = "MIN",
            [CommandOperationType.Sum] = "SUM",
            [CommandOperationType.Avg] = "AVG",
            [CommandOperationType.Count] = "COUNT",
        };

        internal static bool wrapFieldWithQuotes = true;

        internal static bool uppercase = true;

        #endregion

        #region Configure oracle

        /// <summary>
        /// Configure oracle
        /// </summary>
        /// <param name="oracleOptions">Oracle option</param>
        public static void Configure(OracleOptions oracleOptions)
        {
            if (oracleOptions != null)
            {
                wrapFieldWithQuotes = oracleOptions.WrapWithQuotes;
                uppercase = oracleOptions.Uppercase;
            }
        }

        #endregion

        #region Get database connection

        /// <summary>
        /// Get oracle database connection
        /// </summary>
        /// <param name="server">database server</param>
        /// <returns>return database connection</returns>
        internal static IDbConnection GetConnection(DatabaseServer server)
        {
            return DataManager.GetDatabaseConnection(server) ?? new OracleConnection(server.ConnectionString);
        }

        #endregion

        #region Get query translator

        /// <summary>
        /// Get query translator
        /// </summary>
        /// <param name="server">database server</param>
        /// <returns></returns>
        internal static IQueryTranslator GetQueryTranslator(DatabaseServer server)
        {
            return DataManager.GetQueryTranslator(server.ServerType) ?? new OracleQueryTranslator();
        }

        #endregion

        #region Criteria converter

        /// <summary>
        /// Parse criteria converter
        /// </summary>
        /// <param name="converter">converter</param>
        /// <param name="objectName">object name</param>
        /// <param name="fieldName">field name</param>
        /// <returns></returns>
        internal static string ParseCriteriaConverter(ICriteriaConverter converter, string objectName, string fieldName)
        {
            var criteriaConverterParse = DataManager.GetCriteriaConverterParser(converter?.Name) ?? Parse;
            return criteriaConverterParse(new CriteriaConverterParseOptions()
            {
                CriteriaConverter = converter,
                ServerType = DatabaseServerType.Oracle,
                ObjectName = objectName,
                FieldName = fieldName
            });
        }

        /// <summary>
        /// parse
        /// </summary>
        /// <param name="option">parse option</param>
        /// <returns></returns>
        static string Parse(CriteriaConverterParseOptions option)
        {
            if (string.IsNullOrWhiteSpace(option?.CriteriaConverter?.Name))
            {
                throw new EZNEWException("criteria convert config name is null or empty");
            }
            string format = null;
            switch (option.CriteriaConverter.Name)
            {
                case CriteriaConverterNames.StringLength:
                    format = $"LENGTH({option.ObjectName}.{FormatFieldName(option.FieldName)})";
                    break;
            }
            if (string.IsNullOrWhiteSpace(format))
            {
                throw new EZNEWException($"cann't resolve criteria convert:{option.CriteriaConverter.Name} for Oracle");
            }
            return format;
        }

        #endregion

        #region Framework log

        /// <summary>
        /// Log execute command
        /// </summary>
        /// <param name="command">Execte command</param>
        internal static void LogExecutionCommand(DatabaseExecutionCommand command)
        {
            FrameworkLogManager.LogDatabaseExecutionCommand(DatabaseServerType.Oracle, command);
        }

        /// <summary>
        /// Log script
        /// </summary>
        /// <param name="script">Script</param>
        /// <param name="parameter">Parameter</param>
        internal static void LogScript(string script, object parameter)
        {
            FrameworkLogManager.LogDatabaseScript(DatabaseServerType.Oracle, script, parameter);
        }

        #endregion

        #region Get command type

        /// <summary>
        /// Get command type
        /// </summary>
        /// <param name="command">command</param>
        /// <returns></returns>
        internal static CommandType GetCommandType(DefaultCommand command)
        {
            return command.CommandType == CommandTextType.Procedure ? CommandType.StoredProcedure : CommandType.Text;
        }

        #endregion

        #region Get calculate sign

        /// <summary>
        /// Get calculate sign
        /// </summary>
        /// <param name="calculate">calculate operator</param>
        /// <returns></returns>
        internal static string GetCalculateChar(CalculationOperator calculate)
        {
            CalculateOperators.TryGetValue(calculate, out var opearterChar);
            return opearterChar;
        }

        #endregion

        #region Get aggregate function name

        /// <summary>
        /// Get aggregate function name
        /// </summary>
        /// <param name="funcType">function type</param>
        /// <returns></returns>
        internal static string GetAggregateFunctionName(CommandOperationType funcType)
        {
            AggregateFunctions.TryGetValue(funcType, out var funcName);
            return funcName;
        }

        #endregion

        #region Aggregate operate must need field

        /// <summary>
        /// Aggregate operate must need field
        /// </summary>
        /// <param name="operateType">operate type</param>
        /// <returns></returns>
        internal static bool AggregateOperateMustNeedField(CommandOperationType operateType)
        {
            return operateType != CommandOperationType.Count;
        }

        #endregion

        #region Format insert fields

        /// <summary>
        /// Format insert fields
        /// </summary>
        /// <param name="fields">fields</param>
        /// <param name="parameters">parameters</param>
        /// <param name="parameterSequence">parameter sequence</param>
        /// <returns>first:fields,second:parameter fields,third:parameters</returns>
        internal static Tuple<List<string>, List<string>, CommandParameters> FormatInsertFields(int fieldCount, IEnumerable<EntityField> fields, object parameters, int parameterSequence)
        {
            if (fields.IsNullOrEmpty())
            {
                return null;
            }
            List<string> formatFields = new List<string>(fieldCount);
            List<string> parameterFields = new List<string>(fieldCount);
            CommandParameters cmdParameters = ParseParameters(parameters);
            foreach (var field in fields)
            {
                formatFields.Add(FormatFieldName(field.FieldName));

                //parameter name
                parameterSequence++;
                string parameterName = field.PropertyName + parameterSequence;
                parameterFields.Add($"{parameterPrefix}{parameterName}");

                //parameter value
                cmdParameters?.Rename(field.PropertyName, parameterName);
            }
            return new Tuple<List<string>, List<string>, CommandParameters>(formatFields, parameterFields, cmdParameters);
        }

        #endregion

        #region Format fields

        /// <summary>
        /// Format fields
        /// </summary>
        /// <param name="fields">fields</param>
        /// <returns></returns>
        internal static IEnumerable<string> FormatQueryFields(string databasePetName, IQuery query, Type entityType, bool forceMustFields, bool convertField)
        {
            if (query == null || entityType == null)
            {
                return Array.Empty<string>();
            }
            var queryFields = GetQueryFields(query, entityType, forceMustFields);
            return queryFields?.Select(field => FormatField(databasePetName, field, convertField)) ?? Array.Empty<string>();
        }

        internal static IEnumerable<string> FormatQueryFields(string dataBaseObjectName, IEnumerable<EntityField> fields, bool convertField)
        {
            return fields?.Select(field => FormatField(dataBaseObjectName, field, convertField)) ?? Array.Empty<string>();
        }

        #endregion

        #region Format field

        /// <summary>
        /// Format field
        /// </summary>
        /// <param name="dataBaseObjectName">database object name</param>
        /// <param name="field">field</param>
        /// <returns></returns>
        internal static string FormatField(string dataBaseObjectName, EntityField field, bool convertField)
        {
            if (field == null)
            {
                return string.Empty;
            }
            string fieldName = FormatFieldName(field.FieldName);
            var formatValue = $"{dataBaseObjectName}.{fieldName}";
            if (!string.IsNullOrWhiteSpace(field.QueryFormat))
            {
                formatValue = string.Format(field.QueryFormat + " AS {1}", formatValue, WrapKeyword(field.PropertyName));
            }
            else if (field.FieldName != field.PropertyName && convertField)
            {
                formatValue = string.Format("{0} AS {1}", formatValue, WrapKeyword(field.PropertyName));
            }
            return formatValue;
        }

        /// <summary>
        /// Format field name
        /// </summary>
        /// <param name="fieldName">Field name</param>
        /// <returns></returns>
        internal static string FormatFieldName(string fieldName)
        {
            if (string.IsNullOrWhiteSpace(fieldName))
            {
                return string.Empty;
            }
            if (uppercase)
            {
                fieldName = fieldName.ToUpper();
            }
            if (wrapFieldWithQuotes)
            {
                fieldName = $"{WrapKeyword(fieldName)}";
            }
            return fieldName;
        }

        #endregion

        #region Format table name

        internal static string FormatTableName(string originalTableName)
        {
            if (uppercase)
            {
                originalTableName = originalTableName.ToUpper();
            }
            if (wrapFieldWithQuotes)
            {
                originalTableName = $"{WrapKeyword(originalTableName)}";
            }
            return originalTableName;
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

        #region Get fields

        internal static IEnumerable<EntityField> GetQueryFields(IQuery query, Type entityType, bool forceMustFields)
        {
            return DataManager.GetQueryFields(DatabaseServerType.Oracle, entityType, query, forceMustFields);
        }

        /// <summary>
        /// Get fields
        /// </summary>
        /// <param name="entityType">entity type</param>
        /// <param name="propertyNames">property names</param>
        /// <returns></returns>
        internal static IEnumerable<EntityField> GetFields(Type entityType, IEnumerable<string> propertyNames)
        {
            return DataManager.GetFields(DatabaseServerType.Oracle, entityType, propertyNames);
        }

        #endregion

        #region Get default field

        /// <summary>
        /// Get default field
        /// </summary>
        /// <param name="entityType">Entity type</param>
        /// <returns>Return default field name</returns>
        internal static string GetDefaultFieldName(Type entityType)
        {
            if (entityType == null)
            {
                return string.Empty;
            }
            return DataManager.GetDefaultField(DatabaseServerType.Oracle, entityType)?.FieldName ?? string.Empty;
        }

        #endregion

        #region Format parameter name

        /// <summary>
        /// Format parameter name
        /// </summary>
        /// <param name="parameterName">parameter name</param>
        /// <param name="parameterSequence">parameter sequence</param>
        /// <returns></returns>
        internal static string FormatParameterName(string parameterName, int parameterSequence)
        {
            return parameterName + parameterSequence;
        }

        #endregion

        #region Parse parameter

        /// <summary>
        /// Parse parameter
        /// </summary>
        /// <param name="originParameters">origin parameter</param>
        /// <returns></returns>
        internal static CommandParameters ParseParameters(object originParameters)
        {
            if (originParameters == null)
            {
                return null;
            }
            if (originParameters is CommandParameters commandParameters)
            {
                return commandParameters;
            }
            commandParameters = new CommandParameters();
            if (originParameters is IEnumerable<KeyValuePair<string, string>> stringParametersDict)
            {
                commandParameters.Add(stringParametersDict);
            }
            else if (originParameters is IEnumerable<KeyValuePair<string, dynamic>> dynamicParametersDict)
            {
                commandParameters.Add(dynamicParametersDict);
            }
            else if (originParameters is IEnumerable<KeyValuePair<string, object>> objectParametersDict)
            {
                commandParameters.Add(objectParametersDict);
            }
            else if (originParameters is IEnumerable<KeyValuePair<string, IModificationValue>> modifyParametersDict)
            {
                commandParameters.Add(modifyParametersDict);
            }
            else
            {
                objectParametersDict = originParameters.ObjectToDcitionary();
                commandParameters.Add(objectParametersDict);
            }
            return commandParameters;
        }

        #endregion

        #region Convert command parameters

        /// <summary>
        /// Convert command parameters
        /// </summary>
        /// <param name="cmdParameters">command parameters</param>
        /// <returns></returns>
        internal static DynamicParameters ConvertCmdParameters(CommandParameters cmdParameters)
        {
            if (cmdParameters?.Parameters.IsNullOrEmpty() ?? true)
            {
                return null;
            }
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var item in cmdParameters.Parameters)
            {
                var parameter = DataManager.HandleParameter(DatabaseServerType.Oracle, item.Value);
                dynamicParameters.Add(parameter.Name, parameter.Value
                                    , parameter.DbType, parameter.ParameterDirection
                                    , parameter.Size, parameter.Precision
                                    , parameter.Scale);
            }
            return dynamicParameters;
        }

        #endregion

        #region Get transaction isolation level

        /// <summary>
        /// Get transaction isolation level
        /// </summary>
        /// <param name="dataIsolationLevel">data isolation level</param>
        /// <returns></returns>
        internal static IsolationLevel? GetTransactionIsolationLevel(DataIsolationLevel? dataIsolationLevel)
        {
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.Oracle);
            }
            return DataManager.GetSystemIsolationLevel(dataIsolationLevel);
        }

        #endregion

        #region Get query transaction

        /// <summary>
        /// Get query transaction
        /// </summary>
        /// <param name="connection">connection</param>
        /// <param name="query">query</param>
        /// <returns></returns>
        internal static IDbTransaction GetQueryTransaction(IDbConnection connection, IQuery query)
        {
            DataIsolationLevel? dataIsolationLevel = query?.IsolationLevel;
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.Oracle);
            }
            var systemIsolationLevel = GetTransactionIsolationLevel(dataIsolationLevel);
            if (systemIsolationLevel.HasValue)
            {
                if (connection.State != ConnectionState.Open)
                {
                    connection.Open();
                }
                return connection.BeginTransaction(systemIsolationLevel.Value);
            }
            return null;
        }

        #endregion

        #region Get execute transaction

        /// <summary>
        /// Get execute transaction
        /// </summary>
        /// <param name="connection">connection</param>
        /// <param name="executeOption">execute option</param>
        /// <returns></returns>
        internal static IDbTransaction GetExecuteTransaction(IDbConnection connection, CommandExecutionOptions executeOption)
        {
            DataIsolationLevel? dataIsolationLevel = executeOption?.IsolationLevel;
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.Oracle);
            }
            var systemIsolationLevel = DataManager.GetSystemIsolationLevel(dataIsolationLevel);
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return systemIsolationLevel.HasValue ? connection.BeginTransaction(systemIsolationLevel.Value) : connection.BeginTransaction();
        }

        #endregion

        #region Combine condition 

        /// <summary>
        /// Combine condition
        /// </summary>
        /// <param name="normalCondition">Normal condition</param>
        /// <param name="limitCondition">Limit condition</param>
        /// <returns></returns>
        internal static string CombineLimitCondition(string normalCondition, string limitCondition)
        {
            return string.IsNullOrWhiteSpace(normalCondition)
                   ? (string.IsNullOrWhiteSpace(limitCondition) ? string.Empty : $"WHERE {limitCondition}")
                   : (string.IsNullOrWhiteSpace(limitCondition) ? $"WHERE {normalCondition}" : $"WHERE {normalCondition} AND {limitCondition}");
        }

        #endregion
    }
}
