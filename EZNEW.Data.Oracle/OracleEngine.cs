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
                    transaction = OracleFactory.GetExecuteTransaction(conn, executeOption);
                }
                try
                {
                    foreach (var command in executeCommands)
                    {
                        var cmdDefinition = new CommandDefinition(command.CommandText, OracleFactory.ConvertCmdParameters(command.Parameters), transaction: transaction, commandType: command.CommandType, cancellationToken: executeOption?.CancellationToken ?? default);
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
                    Parameters = OracleFactory.ParseParameters(command.Parameters),
                    CommandType = OracleFactory.GetCommandType(command),
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
            int fieldCount = fields.GetCount();
            var insertFormatResult = OracleFactory.FormatInsertFields(fieldCount, fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO {objectName} ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)})";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fieldCount;
            return new DatabaseExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = OracleFactory.GetCommandType(command),
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

            CommandParameters parameters = OracleFactory.ParseParameters(command.Parameters) ?? new CommandParameters();
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
            var fields = OracleFactory.GetFields(command.EntityType, command.Fields);
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
                    parameterName = OracleFactory.FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModifyValue)
                    {
                        var modifyValue = parameterValue as IModifyValue;
                        parameters.ModifyValue(parameterName, modifyValue.Value);
                        if (parameterValue is CalculateModifyValue)
                        {
                            var calculateModifyValue = parameterValue as CalculateModifyValue;
                            string calChar = OracleFactory.GetCalculateChar(calculateModifyValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.{field.FieldName}{calChar}{OracleFactory.parameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{OracleFactory.parameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{translator.ObjectPetName}.{field.FieldName}={newValueExpression}");
            }
            string cmdText = $"{preScript}UPDATE {objectName} {translator.ObjectPetName} {joinScript} SET {string.Join(",", updateSetArray)} {conditionString}";
            translator.ParameterSequence = parameterSequence;

            #endregion

            #region parameter

            var queryParameters = OracleFactory.ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = OracleFactory.GetCommandType(command),
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

            CommandParameters parameters = OracleFactory.ParseParameters(command.Parameters) ?? new CommandParameters();
            var queryParameters = OracleFactory.ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecuteCommand()
            {
                CommandText = cmdText,
                CommandType = OracleFactory.GetCommandType(command),
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

            #region script

            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
                    bool hasOrder = !string.IsNullOrWhiteSpace(tranResult.OrderString);
                    bool hasCombine = !string.IsNullOrWhiteSpace(tranResult.CombineScript);
                    bool hasLimit = command.Query.QuerySize > 0;
                    var orderString = hasOrder ? $"ORDER BY {tranResult.OrderString}" : string.Empty;
                    string limitCondition = hasLimit ? hasCombine || hasOrder ? $"WHERE ROWNUM <= {command.Query.QuerySize}" : $"ROWNUM <= {command.Query.QuerySize}" : string.Empty;
                    var conditionString = OracleFactory.CombineLimitCondition(tranResult.ConditionString, hasCombine || (hasOrder && hasLimit) ? string.Empty : limitCondition);
                    var queryFields = OracleFactory.GetQueryFields(command.Query, command.EntityType, true);
                    var innerFormatedField = string.Join(",", OracleFactory.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                    var outputFormatedField = string.Join(",", OracleFactory.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    if (hasCombine)
                    {
                        cmdText = hasOrder && hasLimit
                            ? $"{tranResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM (SELECT {innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript}) {translator.ObjectPetName} {orderString}) {translator.ObjectPetName} {limitCondition}"
                            : $"{tranResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript}) {translator.ObjectPetName} {orderString} {limitCondition}";
                    }
                    else
                    {
                        cmdText = hasOrder && hasLimit
                            ? $"{tranResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {orderString}) {translator.ObjectPetName} {limitCondition}"
                            : $"{tranResult.PreScript}SELECT {outputFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {orderString}";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = OracleFactory.ConvertCmdParameters(OracleFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            OracleFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = OracleFactory.GetConnection(server))
            {
                var tran = OracleFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: OracleFactory.GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
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

            #region script

            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
                    string defaultFieldName = OracleFactory.GetDefaultFieldName(command.EntityType);
                    int beginRow = offsetNum + 1;
                    string conditionString = string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $" WHERE {tranResult.ConditionString}";
                    string offsetConditionString = $"WHERE EZNEW_ROWNUMBER BETWEEN {beginRow} AND {beginRow + size}";
                    string orderString = string.IsNullOrWhiteSpace(tranResult.OrderString) ? $" ORDER BY {translator.ObjectPetName}.{defaultFieldName} DESC" : $" ORDER BY {tranResult.OrderString}";
                    string totalCountAndRowNumber = $"SELECT COUNT({translator.ObjectPetName}.{defaultFieldName}) OVER() AS QueryDataTotalCount,ROW_NUMBER() OVER({orderString}) AS EZNEW_ROWNUMBER";
                    var queryFields = OracleFactory.GetQueryFields(command.Query, command.EntityType, true);
                    var innerFormatedField = string.Join(",", OracleFactory.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                    var outputFormatedField = string.Join(",", OracleFactory.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    cmdText = string.IsNullOrWhiteSpace(tranResult.CombineScript)
                        ? $"{tranResult.PreScript}SELECT {outputFormatedField} FROM ({totalCountAndRowNumber},{innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString}) {translator.ObjectPetName} {offsetConditionString}"
                        : $"{tranResult.PreScript}SELECT {outputFormatedField} FROM ({totalCountAndRowNumber},{innerFormatedField} FROM (SELECT {innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript}) {translator.ObjectPetName}) {translator.ObjectPetName} {offsetConditionString}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = OracleFactory.ConvertCmdParameters(OracleFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            OracleFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = OracleFactory.GetConnection(server))
            {
                var tran = OracleFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: OracleFactory.GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
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

            command.Query.ClearQueryFields();
            var queryFields = EntityManager.GetPrimaryKeys(command.EntityType).ToArray();
            if (queryFields.IsNullOrEmpty())
            {
                queryFields = EntityManager.GetQueryFields(command.EntityType).ToArray();
            }
            command.Query.AddQueryFields(queryFields);
            var tranResult = translator.Translate(command.Query);
            string conditionString = string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}";
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
            string formatedField = string.Join(",", OracleFactory.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false));
            string cmdText = $"{preScript}SELECT CASE WHEN EXISTS(SELECT {formatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript}) THEN 1 ELSE 0 END FROM DUAL";

            #endregion

            #region parameter

            var parameters = OracleFactory.ConvertCmdParameters(OracleFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            OracleFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = OracleFactory.GetConnection(server))
            {
                var tran = OracleFactory.GetQueryTransaction(conn, command.Query);
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

            bool queryObject = command.Query.QueryType == QueryCommandType.QueryObject;
            string funcName = OracleFactory.GetAggregateFunctionName(command.OperateType);
            EntityField defaultField = null;
            if (queryObject)
            {
                if (string.IsNullOrWhiteSpace(funcName))
                {
                    throw new NotSupportedException($"Not support {command.OperateType}");
                }
                if (OracleFactory.AggregateOperateMustNeedField(command.OperateType))
                {
                    if (command.Query.QueryFields.IsNullOrEmpty())
                    {
                        throw new EZNEWException($"You must specify the field to perform for the {funcName} operation");
                    }
                    defaultField = DataManager.GetField(DatabaseServerType.Oracle, command.EntityType, command.Query.QueryFields.First());
                }
                else
                {
                    defaultField = DataManager.GetDefaultField(DatabaseServerType.Oracle, command.EntityType);
                }

                //combine fields
                if (!command.Query.CombineItems.IsNullOrEmpty())
                {
                    var combineKeys = EntityManager.GetPrimaryKeys(command.EntityType).Union(new string[1] { defaultField.PropertyName }).ToArray();
                    command.Query.ClearQueryFields();
                    foreach (var combineItem in command.Query.CombineItems)
                    {
                        combineItem.CombineQuery.ClearQueryFields();
                        if (combineKeys.IsNullOrEmpty())
                        {
                            combineItem.CombineQuery.ClearNotQueryFields();
                            command.Query.ClearNotQueryFields();
                        }
                        else
                        {
                            combineItem.CombineQuery.AddQueryFields(combineKeys);
                            command.Query.AddQueryFields(combineKeys);
                        }
                    }
                }
            }
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
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.Oracle, command.EntityType, command.ObjectName);
                    var conditionString = string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}";
                    var defaultQueryField = OracleFactory.FormatField(translator.ObjectPetName, defaultField, false);
                    cmdText = string.IsNullOrWhiteSpace(tranResult.CombineScript)
                        ? $"{tranResult.PreScript}SELECT {funcName}({defaultQueryField}) FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString}"
                        : $"{tranResult.PreScript}SELECT {funcName}({defaultQueryField}) FROM (SELECT {string.Join(",", OracleFactory.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript}) {translator.ObjectPetName}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = OracleFactory.ConvertCmdParameters(OracleFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            OracleFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = OracleFactory.GetConnection(server))
            {
                var tran = OracleFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: OracleFactory.GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
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
                var tran = OracleFactory.GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = OracleFactory.ConvertCmdParameters(OracleFactory.ParseParameters(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.CommandText, parameters, transaction: tran, commandType: OracleFactory.GetCommandType(command as RdbCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
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
    }
}
