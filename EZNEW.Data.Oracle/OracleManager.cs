using System;
using System.Data;
using System.Linq;
using System.Collections.Generic;
using Dapper;
using Oracle.ManagedDataAccess.Client;
using EZNEW.Development.Query.Translation;
using EZNEW.Logging;
using EZNEW.Development.Query;
using EZNEW.Development.Command;
using EZNEW.Development.Entity;
using EZNEW.Development.DataAccess;
using EZNEW.Data.ParameterHandler;
using EZNEW.Data.Conversion;
using EZNEW.Exceptions;

namespace EZNEW.Data.Oracle
{
    /// <summary>
    /// Defines oracle manager
    /// </summary>
    public static class OracleManager
    {
        #region Fields

        /// <summary>
        /// Gets current database server type
        /// </summary>
        internal const DatabaseServerType CurrentDatabaseServerType = DatabaseServerType.Oracle;

        /// <summary>
        /// Field format key
        /// </summary>
        internal static readonly string FieldFormatKey = ((int)CurrentDatabaseServerType).ToString();

        /// <summary>
        /// Parameter prefix
        /// </summary>
        internal const string ParameterPrefix = ":";

        /// <summary>
        /// Key word prefix
        /// </summary>
        internal const string KeywordPrefix = "\"";

        /// <summary>
        /// Key word suffix
        /// </summary>
        internal const string KeywordSuffix = "\"";

        /// <summary>
        /// Calculation operators
        /// </summary>
        static readonly Dictionary<CalculationOperator, string> CalculationOperators = new Dictionary<CalculationOperator, string>(4)
        {
            [CalculationOperator.Add] = "+",
            [CalculationOperator.Subtract] = "-",
            [CalculationOperator.Multiply] = "*",
            [CalculationOperator.Divide] = "/",
        };

        /// <summary>
        /// Aggregation functions
        /// </summary>
        static readonly Dictionary<CommandOperationType, string> AggregationFunctions = new Dictionary<CommandOperationType, string>(5)
        {
            [CommandOperationType.Max] = "MAX",
            [CommandOperationType.Min] = "MIN",
            [CommandOperationType.Sum] = "SUM",
            [CommandOperationType.Avg] = "AVG",
            [CommandOperationType.Count] = "COUNT",
        };

        /// <summary>
        /// Oracle options
        /// </summary>
        static readonly OracleOptions OracleOptions = new OracleOptions();

        /// <summary>
        /// Default feild converter
        /// </summary>
        static readonly OracleDefaultFieldConverter DefaultFieldConverter = new OracleDefaultFieldConverter();

        /// <summary>
        /// Database provider type
        /// </summary>
        static readonly Type ProviderType = typeof(OracleProvider);

        #endregion

        #region Configure oracle

        /// <summary>
        /// Configure oracle
        /// </summary>
        /// <param name="configureDelegate">Configure delegate</param>
        public static void Configure(Action<OracleOptions> configureDelegate)
        {
            configureDelegate?.Invoke(OracleOptions);
            if (OracleOptions.FormattingGuid)
            {
                DataManager.AddParameterHandler(CurrentDatabaseServerType, DbType.Guid, new GuidFormattingParameterHandler());
            }
            else
            {
                DataManager.RemoveParameterHandler(CurrentDatabaseServerType, DbType.Guid);
            }
        }

        #endregion

        #region Get database connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns>Return database connection</returns>
        internal static IDbConnection GetConnection(DatabaseServer server)
        {
            return DataManager.GetDatabaseConnection(server) ?? new OracleConnection(server.ConnectionString);
        }

        #endregion

        #region Get query translator

        /// <summary>
        /// Get query translator
        /// </summary>
        /// <param name="dataAccessContext">Data access context</param>
        /// <returns>Return query translator</returns>
        internal static IQueryTranslator GetQueryTranslator(DataAccessContext dataAccessContext)
        {
            if (dataAccessContext?.Server == null)
            {
                throw new ArgumentNullException($"{nameof(DataAccessContext.Server)}");
            }
            var translator = DataManager.GetQueryTranslator(dataAccessContext.Server.ServerType) ?? new OracleQueryTranslator();
            translator.DataAccessContext = dataAccessContext;
            return translator;
        }

        #endregion

        #region Field conversion

        /// <summary>
        /// Convert field
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="conversionOptions">Field conversion options</param>
        /// <param name="objectName">Object name</param>
        /// <param name="fieldName">Field name</param>
        /// <returns>Return field conversion result</returns>
        internal static FieldConversionResult ConvertField(DatabaseServer server, FieldConversionOptions conversionOptions, string objectName, string fieldName)
        {
            if (string.IsNullOrWhiteSpace(conversionOptions?.ConversionName))
            {
                return null;
            }

            IFieldConverter fieldConverter = DataManager.GetFieldConverter(conversionOptions.ConversionName) ?? DefaultFieldConverter;
            return fieldConverter.Convert(new FieldConversionContext()
            {
                ConversionName = conversionOptions.ConversionName,
                Parameter = conversionOptions.Parameter,
                FieldName = fieldName,
                ObjectName = objectName,
                Server = server
            });
        }

        #endregion

        #region Framework log

        /// <summary>
        /// Log execution command
        /// </summary>
        /// <param name="command">Exection command</param>
        internal static void LogExecutionCommand(DatabaseExecutionCommand command)
        {
            FrameworkLogManager.LogDatabaseExecutionCommand(ProviderType,CurrentDatabaseServerType, command);
        }

        /// <summary>
        /// Log script
        /// </summary>
        /// <param name="script">Script</param>
        /// <param name="parameter">Parameter</param>
        internal static void LogScript(string script, object parameter)
        {
            FrameworkLogManager.LogDatabaseScript(ProviderType,CurrentDatabaseServerType, script, parameter);
        }

        #endregion

        #region Get command type

        /// <summary>
        /// Get command type
        /// </summary>
        /// <param name="command">Command</param>
        /// <returns>Return command type</returns>
        internal static CommandType GetCommandType(DefaultCommand command)
        {
            return command.TextType == CommandTextType.Procedure ? CommandType.StoredProcedure : CommandType.Text;
        }

        #endregion

        #region Get system calculation operator

        /// <summary>
        /// Get system calculation operator
        /// </summary>
        /// <param name="calculationOperator">Calculation operator</param>
        /// <returns>Return system calculation operator</returns>
        internal static string GetSystemCalculationOperator(CalculationOperator calculationOperator)
        {
            CalculationOperators.TryGetValue(calculationOperator, out var systemCalculationOperator);
            return systemCalculationOperator;
        }

        #endregion

        #region Get aggregation function name

        /// <summary>
        /// Get aggregation function name
        /// </summary>
        /// <param name="commandOperationType">Command operation type</param>
        /// <returns>Return aggregation function name</returns>
        internal static string GetAggregationFunctionName(CommandOperationType commandOperationType)
        {
            AggregationFunctions.TryGetValue(commandOperationType, out var funcName);
            return funcName;
        }

        #endregion

        #region Check aggregation operation whether must need field

        /// <summary>
        /// Check aggregation operation whether must need field
        /// </summary>
        /// <param name="operationType">Operation type</param>
        /// <returns>Whether must need field</returns>
        internal static bool CheckAggregationOperationMustNeedField(CommandOperationType operationType)
        {
            return operationType != CommandOperationType.Count;
        }

        #endregion

        #region Format insertion fields

        /// <summary>
        /// Format insertion fields
        /// </summary>
        /// <param name="entityType">Entity type</param>
        /// <param name="fields">Fields</param>
        /// <param name="parameters">Parameters</param>
        /// <param name="parameterSequence">Parameter sequence</param>
        /// <returns>First:fields,Second:parameter fields,Third:parameters</returns>
        internal static Tuple<List<string>, List<string>, CommandParameters> FormatInsertionFields(Type entityType, int fieldCount, IEnumerable<EntityField> fields, CommandParameters parameters, int parameterSequence)
        {
            if (fields.IsNullOrEmpty())
            {
                throw new EZNEWException($"Entity type {entityType?.Name} not set fields for insertion.");
            }
            List<string> formatFields = new List<string>(fieldCount);
            List<string> parameterFields = new List<string>(fieldCount);
            foreach (var field in fields)
            {
                formatFields.Add(FormatFieldName(field.FieldName));

                //parameter name
                parameterSequence++;
                string parameterName = field.PropertyName + parameterSequence;
                parameterFields.Add($"{ParameterPrefix}{parameterName}");

                //parameter value
                parameters?.Rename(field.PropertyName, parameterName);
            }
            return new Tuple<List<string>, List<string>, CommandParameters>(formatFields, parameterFields, parameters);
        }

        #endregion

        #region Format fields

        /// <summary>
        /// Format query fields
        /// </summary>
        /// <param name="objectPetName">Object pet name</param>
        /// <param name="query">Query</param>
        /// <param name="entityType">Entity type</param>
        /// <param name="forceNecessaryFields">Whether force include necessary fields</param>
        /// <param name="conversionFieldName">Indicates whether conversion field name</param>
        /// <returns></returns>
        internal static IEnumerable<string> FormatQueryFields(string objectPetName, IQuery query, Type entityType, bool forceNecessaryFields, bool conversionFieldName)
        {
            if (query == null || entityType == null)
            {
                return Array.Empty<string>();
            }
            var queryFields = GetQueryFields(query, entityType, forceNecessaryFields);
            return queryFields?.Select(field => FormatField(objectPetName, field, conversionFieldName)) ?? Array.Empty<string>();
        }

        /// <summary>
        /// Format query fields
        /// </summary>
        /// <param name="objectPetName">Object pet name</param>
        /// <param name="fields">Fields</param>
        /// <param name="conversionFieldName">Whether convert field</param>
        /// <returns></returns>
        internal static IEnumerable<string> FormatQueryFields(string objectPetName, IEnumerable<EntityField> fields, bool conversionFieldName)
        {
            return fields?.Select(field => FormatField(objectPetName, field, conversionFieldName)) ?? Array.Empty<string>();
        }

        #endregion

        #region Format field

        /// <summary>
        /// Format field
        /// </summary>
        /// <param name="objectPetName">Object pet name</param>
        /// <param name="field">Field</param>
        /// <param name="conversionFieldName">Whether conversion field name</param>
        /// <returns></returns>
        internal static string FormatField(string objectPetName, EntityField field, bool conversionFieldName)
        {
            if (field == null)
            {
                return string.Empty;
            }
            string fieldName = FormatFieldName(field.FieldName);
            var formatValue = $"{objectPetName}.{fieldName}";
            if (!string.IsNullOrWhiteSpace(field.QueryFormat))
            {
                formatValue = string.Format(field.QueryFormat + " AS {1}", formatValue, WrapKeyword(field.PropertyName));
            }
            else if (field.FieldName != field.PropertyName && conversionFieldName)
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
            if (OracleOptions.Uppercase)
            {
                fieldName = fieldName.ToUpper();
            }
            if (OracleOptions.WrapWithQuotes)
            {
                fieldName = $"{WrapKeyword(fieldName)}";
            }
            return fieldName;
        }

        #endregion

        #region Format table name

        internal static string FormatTableName(string originalTableName)
        {
            if (OracleOptions.Uppercase)
            {
                originalTableName = originalTableName.ToUpper();
            }
            if (OracleOptions.WrapWithQuotes)
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

        /// <summary>
        /// Get query fields
        /// </summary>
        /// <param name="query">Query</param>
        /// <param name="entityType">Entity type</param>
        /// <param name="forceNecessaryFields">Whether include necessary fields</param>
        /// <returns></returns>
        internal static IEnumerable<EntityField> GetQueryFields(IQuery query, Type entityType, bool forceNecessaryFields)
        {
            return DataManager.GetQueryFields(CurrentDatabaseServerType, entityType, query, forceNecessaryFields);
        }

        /// <summary>
        /// Get fields
        /// </summary>
        /// <param name="entityType">entity type</param>
        /// <param name="propertyNames">property names</param>
        /// <returns></returns>
        internal static IEnumerable<EntityField> GetFields(Type entityType, IEnumerable<string> propertyNames)
        {
            return DataManager.GetFields(CurrentDatabaseServerType, entityType, propertyNames);
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
            return DataManager.GetDefaultField(CurrentDatabaseServerType, entityType)?.FieldName ?? string.Empty;
        }

        #endregion

        #region Format parameter name

        /// <summary>
        /// Format parameter name
        /// </summary>
        /// <param name="parameterName">Parameter name</param>
        /// <param name="parameterSequence">Parameter sequence</param>
        /// <returns></returns>
        internal static string FormatParameterName(string parameterName, int parameterSequence)
        {
            return parameterName + parameterSequence;
        }

        #endregion

        #region Convert parameter

        /// <summary>
        /// Convert parameter
        /// </summary>
        /// <param name="originalParameter">Original parameter</param>
        /// <returns>Return command parameters</returns>
        internal static CommandParameters ConvertParameter(object originalParameter)
        {
            return CommandParameters.Parse(originalParameter);
        }

        #endregion

        #region Convert command parameters

        /// <summary>
        /// Convert command parameters
        /// </summary>
        /// <param name="commandParameters">Command parameters</param>
        /// <returns>Return dynamic parameters</returns>
        internal static DynamicParameters ConvertCmdParameters(CommandParameters commandParameters)
        {
            return commandParameters?.ConvertToDynamicParameters(CurrentDatabaseServerType);
        }

        #endregion

        #region Get transaction isolation level

        /// <summary>
        /// Get transaction isolation level
        /// </summary>
        /// <param name="dataIsolationLevel">Data isolation level</param>
        /// <returns></returns>
        internal static IsolationLevel? GetTransactionIsolationLevel(DataIsolationLevel? dataIsolationLevel)
        {
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(CurrentDatabaseServerType);
            }
            return DataManager.GetSystemIsolationLevel(dataIsolationLevel);
        }

        #endregion

        #region Get query transaction

        /// <summary>
        /// Get query transaction
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="query">Query object</param>
        /// <returns></returns>
        internal static IDbTransaction GetQueryTransaction(IDbConnection connection, IQuery query)
        {
            DataIsolationLevel? dataIsolationLevel = query?.IsolationLevel;
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(CurrentDatabaseServerType);
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

        #region Get execution transaction

        /// <summary>
        /// Get execution transaction
        /// </summary>
        /// <param name="connection">Database connection</param>
        /// <param name="executionOptions">Execution options</param>
        /// <returns></returns>
        internal static IDbTransaction GetExecutionTransaction(IDbConnection connection, CommandExecutionOptions executionOptions)
        {
            DataIsolationLevel? dataIsolationLevel = executionOptions?.IsolationLevel;
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(CurrentDatabaseServerType);
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
