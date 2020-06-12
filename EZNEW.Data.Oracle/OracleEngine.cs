using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EZNEW.Develop.Command.Modify;
using EZNEW.Fault;
using EZNEW.Develop.DataAccess;
using EZNEW.Develop.Entity;
using EZNEW.Develop.CQuery;
using EZNEW.Develop.CQuery.Translator;
using EZNEW.Develop.Command;
using EZNEW.Dapper;
using EZNEW.Data.Configuration;

namespace EZNEW.Data.Oracle
{
    /// <summary>
    /// Imeplements database engine for oracle
    /// </summary>
    public class OracleEngine : IDatabaseEngine
    {
        static readonly string fieldFormatKey = ((int)DatabaseServerType.Oracle).ToString();
        const string parameterPrefix = ":";
        static readonly Dictionary<CalculateOperator, string> CalculateOperatorDictionary = new Dictionary<CalculateOperator, string>(4)
        {
            [CalculateOperator.Add] = "+",
            [CalculateOperator.Subtract] = "-",
            [CalculateOperator.Multiply] = "*",
            [CalculateOperator.Divide] = "/",
        };

        static readonly Dictionary<OperateType, string> AggregateFunctionDictionary = new Dictionary<OperateType, string>(5)
        {
            [OperateType.Max] = "MAX",
            [OperateType.Min] = "MIN",
            [OperateType.Sum] = "SUM",
            [OperateType.Avg] = "AVG",
            [OperateType.Count] = "COUNT",
        };

        #region Execute

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="executeOption">execute option</param>
        /// <param name="commands">commands</param>
        /// <returns>data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecuteOption executeOption, IEnumerable<ICommand> commands)
        {
            return ExecuteAsync(server, executeOption, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="executeOption">execute option</param>
        /// <param name="commands">commands</param>
        /// <returns>data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecuteOption executeOption, params ICommand[] commands)
        {
            return ExecuteAsync(server, executeOption, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="executeOption">execute option</param>
        /// <param name="commands">commands</param>
        /// <returns>data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecuteOption executeOption, IEnumerable<ICommand> commands)
        {
            #region group execute commands

            IQueryTranslator translator = OracleFactory.GetQueryTranslator(server);
            List<DatabaseExecuteCommand> executeCommands = new List<DatabaseExecuteCommand>();
            var batchExecuteConfig = DataManager.GetBatchExecuteConfiguration(server.ServerType) ?? BatchExecuteConfiguration.Default;
            var groupStatementsCount = batchExecuteConfig.GroupStatementsCount;
            groupStatementsCount = groupStatementsCount < 0 ? 1 : groupStatementsCount;
            var groupParameterCount = batchExecuteConfig.GroupParametersCount;
            groupParameterCount = groupParameterCount < 0 ? 1 : groupParameterCount;
            StringBuilder commandTextBuilder = new StringBuilder();
            CommandParameters parameters = null;
            int statementsCount = 0;
            bool forceReturnValue = false;
            int cmdCount = 0;

            DatabaseExecuteCommand GetGroupExecuteCommand()
            {
                var executeCommand = new DatabaseExecuteCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    ForceReturnValue = forceReturnValue,
                    Parameters = parameters
                };
                statementsCount = 0;
                translator.ParameterSequence = 0;
                commandTextBuilder.Clear();
                parameters = null;
                forceReturnValue = false;
                return executeCommand;
            }

            foreach (var command in commands)
            {
                DatabaseExecuteCommand executeCommand = GetExecuteDbCommand(translator, command as RdbCommand);
                if (executeCommand == null)
                {
                    continue;
                }

                //Trace log
                OracleFactory.LogExecuteCommand(executeCommand);

                cmdCount++;
                if (executeCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        executeCommands.Add(GetGroupExecuteCommand());
                    }
                    executeCommands.Add(executeCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(executeCommand.CommandText);
                parameters = parameters == null ? executeCommand.Parameters : parameters.Union(executeCommand.Parameters);
                forceReturnValue |= executeCommand.ForceReturnValue;
                statementsCount++;
                if (translator.ParameterSequence >= groupParameterCount || statementsCount >= groupStatementsCount)
                {
                    executeCommands.Add(GetGroupExecuteCommand());
                }
            }
            if (statementsCount > 0)
            {
                executeCommands.Add(GetGroupExecuteCommand());
            }

            #endregion

            return await ExecuteCommandAsync(server, executeOption, executeCommands, executeOption?.ExecuteByTransaction ?? cmdCount > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="executeOption">execute option</param>
        /// <param name="commands">commands</param>
        /// <returns>data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecuteOption executeOption, params ICommand[] commands)
        {
            IEnumerable<ICommand> cmdCollection = commands;
            return await ExecuteAsync(server, executeOption, cmdCollection).ConfigureAwait(false);
        }

        /// <summary>
        /// execute commands
        /// </summary>
        /// <param name="server">db server</param>
        /// <param name="executeOption">execute option</param>
        /// <param name="executeCommands">execute commands</param>
        /// <param name="useTransaction">use transaction</param>
        /// <returns></returns>
        async Task<int> ExecuteCommandAsync(DatabaseServer server, CommandExecuteOption executeOption, IEnumerable<DatabaseExecuteCommand> executeCommands, bool useTransaction)
        {
            int resultValue = 0;
            bool success = true;
            using (var conn = OracleFactory.GetConnection(server))
            {
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = GetExecuteTransaction(conn, executeOption);
                }
                try
                {
                    foreach (var command in executeCommands)
                    {
                        var cmdDefinition = new CommandDefinition(command.CommandText, ConvertCmdParameters(command.Parameters), transaction: transaction, commandType: command.CommandType, cancellationToken: executeOption?.CancellationToken ?? default);
                        var executeResultValue = await conn.ExecuteAsync(cmdDefinition).ConfigureAwait(false);
                        success = success && (command.ForceReturnValue ? executeResultValue > 0 : true);
                        resultValue += executeResultValue;
                        if (useTransaction && !success)
                        {
                            break;
                        }
                    }
                    if (!useTransaction)
                    {
                        return resultValue;
                    }
                    if (success)
                    {
                        transaction.Commit();
                    }
                    else
                    {
                        resultValue = 0;
                        transaction.Rollback();
                    }
                    return resultValue;
                }
                catch (Exception ex)
                {
                    resultValue = 0;
                    transaction?.Rollback();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Get database execute command
        /// </summary>
        /// <param name="command">command</param>
        /// <returns></returns>
        DatabaseExecuteCommand GetExecuteDbCommand(IQueryTranslator queryTranslator, RdbCommand command)
        {
            DatabaseExecuteCommand GetTextCommand()
            {
                return new DatabaseExecuteCommand()
                {
                    CommandText = command.CommandText,
                    Parameters = ParseParameters(command.Parameters),
                    CommandType = GetCommandType(command),
                    ForceReturnValue = command.MustReturnValueOnSuccess,
                    HasPreScript = true
                };
            }
            if (command.ExecuteMode == CommandExecuteMode.CommandText)
            {
                return GetTextCommand();
            }
            DatabaseExecuteCommand executeCommand = null;
            switch (command.OperateType)
            {
                case OperateType.Insert:
                    executeCommand = GetInsertExecuteDbCommand(queryTranslator, command);
                    break;
                case OperateType.Update:
                    executeCommand = GetUpdateExecuteDbCommand(queryTranslator, command);
                    break;
                case OperateType.Delete:
                    executeCommand = GetDeleteExecuteDbCommand(queryTranslator, command);
                    break;
                default:
                    executeCommand = GetTextCommand();
                    break;
            }
            executeCommand.HasPreScript = true;
            return executeCommand;
        }

        /// <summary>
        /// Get insert execute command
        /// </summary>
        /// <param name="translator">translator</param>
        /// <param name="command">command</param>
        /// <returns></returns>
        DatabaseExecuteCommand GetInsertExecuteDbCommand(IQueryTranslator translator, RdbCommand command)
        {
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
            var fields = DataManager.GetEditFields(DatabaseServerType.Oracle, command.EntityType);
            var insertFormatResult = FormatInsertFields(fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO {objectName} ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)})";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fields.Count;
            return new DatabaseExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters
            };
        }

        /// <summary>
        /// Get update execute command
        /// </summary>
        /// <param name="translator">translator</param>
        /// <param name="command">command</param>
        /// <returns></returns>
        DatabaseExecuteCommand GetUpdateExecuteDbCommand(IQueryTranslator translator, RdbCommand command)
        {
            #region query translate

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            CommandParameters parameters = ParseParameters(command.Parameters) ?? new CommandParameters();
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
            var fields = GetFields(command.EntityType, command.Fields);
            int parameterSequence = translator.ParameterSequence;
            List<string> updateSetArray = new List<string>();
            foreach (var field in fields)
            {
                var parameterValue = parameters.GetParameterValue(field.PropertyName);
                var parameterName = field.PropertyName;
                string newValueExpression = string.Empty;
                if (parameterValue != null)
                {
                    parameterSequence++;
                    parameterName = FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModifyValue)
                    {
                        var modifyValue = parameterValue as IModifyValue;
                        parameters.ModifyValue(parameterName, modifyValue.Value);
                        if (parameterValue is CalculateModifyValue)
                        {
                            var calculateModifyValue = parameterValue as CalculateModifyValue;
                            string calChar = GetCalculateChar(calculateModifyValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.{field.FieldName}{calChar}{parameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{parameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{translator.ObjectPetName}.{field.FieldName}={newValueExpression}");
            }
            string cmdText = $"{preScript}UPDATE {objectName} {translator.ObjectPetName} {joinScript} SET {string.Join(",", updateSetArray)} {conditionString}";
            translator.ParameterSequence = parameterSequence;

            #endregion

            #region parameter

            var queryParameters = ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// Get delete execute command
        /// </summary>
        /// <param name="translator">translator</param>
        /// <param name="command">command</param>
        /// <returns></returns>
        DatabaseExecuteCommand GetDeleteExecuteDbCommand(IQueryTranslator translator, RdbCommand command)
        {
            #region query translate

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
            string cmdText = $"{preScript}DELETE {objectName} {translator.ObjectPetName} {joinScript} {conditionString}";

            #endregion

            #region parameter

            CommandParameters parameters = ParseParameters(command.Parameters) ?? new CommandParameters();
            var queryParameters = ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        #endregion

        #region Query

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="command">command</param>
        /// <returns>return datas</returns>
        public IEnumerable<T> Query<T>(DatabaseServer server, ICommand command)
        {
            return QueryAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="command">command</param>
        /// <returns>return datas</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = OracleFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region execute

            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    if (command.Query.QuerySize > 0)
                    {
                        string topSizeCondition = $"ROWNUM <= {command.Query.QuerySize}";
                        if (string.IsNullOrWhiteSpace(tranResult.ConditionString))
                        {
                            tranResult.ConditionString = topSizeCondition;
                        }
                        else
                        {
                            tranResult.ConditionString = $"{tranResult.ConditionString} AND {topSizeCondition}";
                        }
                    }
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
                    cmdText = $"{tranResult.PreScript}SELECT {string.Join(",", FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, out var defaultFieldName))} FROM {objectName} {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} ORDER BY {(string.IsNullOrWhiteSpace(tranResult.OrderString) ? $"{defaultFieldName} DESC" : tranResult.OrderString)}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = ConvertCmdParameters(ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            OracleFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = OracleFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data paging
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">databse server</param>
        /// <param name="command">command</param>
        /// <returns></returns>
        public IEnumerable<T> QueryPaging<T>(DatabaseServer server, ICommand command)
        {
            return QueryPagingAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query data paging
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">databse server</param>
        /// <param name="command">command</param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryPagingAsync<T>(DatabaseServer server, ICommand command)
        {
            int beginIndex = 0;
            int pageSize = 1;
            if (command?.Query?.PagingInfo != null)
            {
                beginIndex = command.Query.PagingInfo.Page;
                pageSize = command.Query.PagingInfo.PageSize;
                beginIndex = (beginIndex - 1) * pageSize;
            }
            return await QueryOffsetAsync<T>(server, command, beginIndex, pageSize).ConfigureAwait(false);
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="command">command</param>
        /// <param name="offsetNum">offset num</param>
        /// <param name="size">query size</param>
        /// <returns></returns>
        public IEnumerable<T> QueryOffset<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            return QueryOffsetAsync<T>(server, command, offsetNum, size).Result;
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="command">command</param>
        /// <param name="offsetNum">offset num</param>
        /// <param name="size">query size</param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> QueryOffsetAsync<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = OracleFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region execute

            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string conditionString = string.Empty;
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
                    List<string> formatQueryFields = FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, out var defaultFieldName);
                    var beginRow = offsetNum + 1;
                    if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
                    {
                        conditionString = $" WHERE {tranResult.ConditionString}";
                    }
                    string orderString;
                    if (string.IsNullOrWhiteSpace(tranResult.OrderString))
                    {
                        orderString = $" ORDER BY {translator.ObjectPetName}.{defaultFieldName} DESC";
                    }
                    else
                    {
                        orderString = $" ORDER BY {tranResult.OrderString}";
                    }
                    cmdText = $"{tranResult.PreScript}SELECT * FROM (SELECT COUNT({translator.ObjectPetName}.{defaultFieldName}) OVER() AS QueryDataTotalCount,ROW_NUMBER() OVER({orderString}) AS EZNEW_ROWNUMBER,{string.Join(",", formatQueryFields)} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString}) WHERE EZNEW_ROWNUMBER BETWEEN {beginRow} AND {beginRow + size}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = ConvertCmdParameters(ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            OracleFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = OracleFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Determine whether data has existed
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="command">command</param>
        /// <returns>data has existed</returns>
        public bool Query(DatabaseServer server, ICommand command)
        {
            return QueryAsync(server, command).Result;
        }

        /// <summary>
        /// Determine whether data has existed
        /// </summary>
        /// <param name="server">server</param>
        /// <param name="command">command</param>
        /// <returns>data has existed</returns>
        public async Task<bool> QueryAsync(DatabaseServer server, ICommand command)
        {
            var translator = OracleFactory.GetQueryTranslator(server);

            #region query translate

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString = $"WHERE {tranResult.ConditionString}";
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            var field = DataManager.GetDefaultField(DatabaseServerType.Oracle, command.EntityType);
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
            string cmdText = $"{preScript}SELECT CASE WHEN EXISTS(SELECT {translator.ObjectPetName}.{field.FieldName} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString}) THEN 1 ELSE 0 END FROM DUAL";

            #endregion

            #region parameter

            var parameters = ConvertCmdParameters(ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            OracleFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = OracleFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, cancellationToken: command.Query?.GetCancellationToken() ?? default);
                int value = await conn.ExecuteScalarAsync<int>(cmdDefinition).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// Query single value
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="command">command</param>
        /// <returns>query data</returns>
        public T AggregateValue<T>(DatabaseServer server, ICommand command)
        {
            return AggregateValueAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query single value
        /// </summary>
        /// <typeparam name="T">data type</typeparam>
        /// <param name="server">database server</param>
        /// <param name="command">command</param>
        /// <returns>query data</returns>
        public async Task<T> AggregateValueAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = OracleFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string funcName = GetAggregateFunctionName(command.OperateType);
                    if (string.IsNullOrWhiteSpace(funcName))
                    {
                        return default;
                    }

                    #region field

                    EntityField field;
                    if (AggregateOperateMustNeedField(command.OperateType))
                    {
                        if (command.Query.QueryFields.IsNullOrEmpty())
                        {
                            throw new EZNEWException($"You must specify the field to perform for the {funcName} operation");
                        }
                        else
                        {
                            field = DataManager.GetField(DatabaseServerType.Oracle, command.EntityType, command.Query.QueryFields.First());
                        }
                    }
                    else
                    {
                        field = DataManager.GetDefaultField(DatabaseServerType.Oracle, command.EntityType);
                    }

                    #endregion

                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
                    cmdText = $"{tranResult.PreScript}SELECT {funcName}({FormatField(translator.ObjectPetName, field)}) FROM {objectName} {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {(string.IsNullOrWhiteSpace(tranResult.OrderString) ? string.Empty : $"ORDER BY {tranResult.OrderString}")}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = ConvertCmdParameters(ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            OracleFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = OracleFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.ExecuteScalarAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data set
        /// </summary>
        /// <param name="server">database server</param>
        /// <param name="command">query command</param>
        /// <returns>return data set</returns>
        public async Task<DataSet> QueryMultipleAsync(DatabaseServer server, ICommand command)
        {
            //Trace log
            OracleFactory.LogScript(command.CommandText, command.Parameters);
            using (var conn = OracleFactory.GetConnection(server))
            {
                var tran = GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = ConvertCmdParameters(ParseParameters(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.CommandText, parameters, transaction: tran, commandType: GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                using (var reader = await conn.ExecuteReaderAsync(cmdDefinition).ConfigureAwait(false))
                {
                    DataSet dataSet = new DataSet();
                    while (!reader.IsClosed && reader.Read())
                    {
                        DataTable dataTable = new DataTable();
                        dataTable.Load(reader);
                        dataSet.Tables.Add(dataTable);
                    }
                    return dataSet;
                }
            }
        }

        #endregion

        #region Util

        /// <summary>
        /// Get command type
        /// </summary>
        /// <param name="command">command</param>
        /// <returns></returns>
        CommandType GetCommandType(RdbCommand command)
        {
            return command.CommandType == CommandTextType.Procedure ? CommandType.StoredProcedure : CommandType.Text;
        }

        /// <summary>
        /// Get calculate sign
        /// </summary>
        /// <param name="calculate">calculate operator</param>
        /// <returns></returns>
        string GetCalculateChar(CalculateOperator calculate)
        {
            CalculateOperatorDictionary.TryGetValue(calculate, out var opearterChar);
            return opearterChar;
        }

        /// <summary>
        /// Get aggregate function name
        /// </summary>
        /// <param name="funcType">function type</param>
        /// <returns></returns>
        string GetAggregateFunctionName(OperateType funcType)
        {
            AggregateFunctionDictionary.TryGetValue(funcType, out var funcName);
            return funcName;
        }

        /// <summary>
        /// Aggregate operate must need field
        /// </summary>
        /// <param name="operateType">operate type</param>
        /// <returns></returns>
        bool AggregateOperateMustNeedField(OperateType operateType)
        {
            return operateType != OperateType.Count;
        }

        /// <summary>
        /// Format insert fields
        /// </summary>
        /// <param name="fields">fields</param>
        /// <param name="parameters">parameters</param>
        /// <param name="parameterSequence">parameter sequence</param>
        /// <returns>first:fields,second:parameter fields,third:parameters</returns>
        Tuple<List<string>, List<string>, CommandParameters> FormatInsertFields(List<EntityField> fields, object parameters, int parameterSequence)
        {
            if (fields.IsNullOrEmpty())
            {
                return null;
            }
            List<string> formatFields = new List<string>(fields.Count);
            List<string> parameterFields = new List<string>(fields.Count);
            CommandParameters cmdParameters = ParseParameters(parameters);
            foreach (var field in fields)
            {
                //fields
                var formatValue = field.GetEditFormat(fieldFormatKey);
                if (string.IsNullOrWhiteSpace(formatValue))
                {
                    formatValue = $"{field.FieldName}";
                    field.SetEditFormat(fieldFormatKey, formatValue);
                }
                formatFields.Add(formatValue);

                //parameter name
                parameterSequence++;
                string parameterName = field.PropertyName + parameterSequence;
                parameterFields.Add($"{parameterPrefix}{parameterName}");

                //parameter value
                cmdParameters?.Rename(field.PropertyName, parameterName);
            }
            return new Tuple<List<string>, List<string>, CommandParameters>(formatFields, parameterFields, cmdParameters);
        }

        /// <summary>
        /// Format fields
        /// </summary>
        /// <param name="fields">fields</param>
        /// <returns></returns>
        List<string> FormatQueryFields(string dataBaseObjectName, IQuery query, Type entityType, out string defaultFieldName)
        {
            defaultFieldName = string.Empty;
            if (query == null || entityType == null)
            {
                return new List<string>(0);
            }
            var queryFields = DataManager.GetQueryFields(DatabaseServerType.Oracle, entityType, query);
            if (queryFields.IsNullOrEmpty())
            {
                return new List<string>(0);
            }
            defaultFieldName = queryFields[0].FieldName;
            List<string> formatFields = new List<string>();
            string key = ((int)DatabaseServerType.Oracle).ToString();
            foreach (var field in queryFields)
            {
                var formatValue = FormatField(dataBaseObjectName, field);
                formatFields.Add(formatValue);
            }
            return formatFields;
        }

        /// <summary>
        /// Format field
        /// </summary>
        /// <param name="dataBaseObjectName">database object name</param>
        /// <param name="field">field</param>
        /// <returns></returns>
        string FormatField(string dataBaseObjectName, EntityField field)
        {
            if (field == null)
            {
                return string.Empty;
            }
            var formatValue = field.GetQueryFormat(fieldFormatKey);
            if (string.IsNullOrWhiteSpace(formatValue))
            {
                string fieldName = $"{dataBaseObjectName}.{field.FieldName}";
                if (!string.IsNullOrWhiteSpace(field.QueryFormat))
                {
                    formatValue = string.Format(field.QueryFormat + " AS \"{1}\"", fieldName, field.PropertyName);
                }
                else if (field.FieldName != field.PropertyName)
                {
                    formatValue = string.Format("{0} AS \"{1}\"", fieldName, field.PropertyName);
                }
                else
                {
                    formatValue = fieldName;
                }
                field.SetQueryFormat(fieldFormatKey, formatValue);
            }
            return formatValue;
        }

        /// <summary>
        /// Get fields
        /// </summary>
        /// <param name="entityType">entity type</param>
        /// <param name="propertyNames">property names</param>
        /// <returns></returns>
        List<EntityField> GetFields(Type entityType, IEnumerable<string> propertyNames)
        {
            return DataManager.GetFields(DatabaseServerType.Oracle, entityType, propertyNames);
        }

        /// <summary>
        /// Format parameter name
        /// </summary>
        /// <param name="parameterName">parameter name</param>
        /// <param name="parameterSequence">parameter sequence</param>
        /// <returns></returns>
        static string FormatParameterName(string parameterName, int parameterSequence)
        {
            return parameterName + parameterSequence;
        }

        /// <summary>
        /// Parse parameter
        /// </summary>
        /// <param name="originParameters">origin parameter</param>
        /// <returns></returns>
        CommandParameters ParseParameters(object originParameters)
        {
            if (originParameters == null)
            {
                return null;
            }
            CommandParameters parameters = originParameters as CommandParameters;
            if (parameters != null)
            {
                return parameters;
            }
            parameters = new CommandParameters();
            if (originParameters is IEnumerable<KeyValuePair<string, string>>)
            {
                var stringParametersDict = originParameters as IEnumerable<KeyValuePair<string, string>>;
                parameters.Add(stringParametersDict);
            }
            else if (originParameters is IEnumerable<KeyValuePair<string, dynamic>>)
            {
                var dynamicParametersDict = originParameters as IEnumerable<KeyValuePair<string, dynamic>>;
                parameters.Add(dynamicParametersDict);
            }
            else if (originParameters is IEnumerable<KeyValuePair<string, object>>)
            {
                var objectParametersDict = originParameters as IEnumerable<KeyValuePair<string, object>>;
                parameters.Add(objectParametersDict);
            }
            else if (originParameters is IEnumerable<KeyValuePair<string, IModifyValue>>)
            {
                var modifyParametersDict = originParameters as IEnumerable<KeyValuePair<string, IModifyValue>>;
                parameters.Add(modifyParametersDict);
            }
            else
            {
                var objectParametersDict = originParameters.ObjectToDcitionary();
                parameters.Add(objectParametersDict);
            }
            return parameters;
        }

        /// <summary>
        /// Convert command parameters
        /// </summary>
        /// <param name="cmdParameters">command parameters</param>
        /// <returns></returns>
        DynamicParameters ConvertCmdParameters(CommandParameters cmdParameters)
        {
            if (cmdParameters?.Parameters.IsNullOrEmpty() ?? true)
            {
                return null;
            }
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (var item in cmdParameters.Parameters)
            {
                var parameter = item.Value;
                if ((parameter.DbType.HasValue && parameter.DbType.Value == DbType.Boolean) || (parameter.Value != null && parameter.Value is bool))
                {
                    parameter.DbType = DbType.Int32;
                    bool.TryParse(parameter.Value?.ToString(), out var boolVal);
                    parameter.Value = boolVal ? 1 : 0;
                }
                dynamicParameters.Add(parameter.Name, parameter.Value
                                    , parameter.DbType, parameter.ParameterDirection
                                    , parameter.Size, parameter.Precision
                                    , parameter.Scale);
            }
            return dynamicParameters;
        }

        /// <summary>
        /// Get transaction isolation level
        /// </summary>
        /// <param name="dataIsolationLevel">data isolation level</param>
        /// <returns></returns>
        IsolationLevel? GetTransactionIsolationLevel(DataIsolationLevel? dataIsolationLevel)
        {
            if (!dataIsolationLevel.HasValue)
            {
                dataIsolationLevel = DataManager.GetDataIsolationLevel(DatabaseServerType.Oracle);
            }
            return DataManager.GetSystemIsolationLevel(dataIsolationLevel);
        }

        /// <summary>
        /// Get query transaction
        /// </summary>
        /// <param name="connection">connection</param>
        /// <param name="query">query</param>
        /// <returns></returns>
        IDbTransaction GetQueryTransaction(IDbConnection connection, IQuery query)
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

        /// <summary>
        /// Get execute transaction
        /// </summary>
        /// <param name="connection">connection</param>
        /// <param name="executeOption">execute option</param>
        /// <returns></returns>
        IDbTransaction GetExecuteTransaction(IDbConnection connection, CommandExecuteOption executeOption)
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
    }
}
