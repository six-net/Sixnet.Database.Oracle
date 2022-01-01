using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Oracle.ManagedDataAccess.Client;
using EZNEW.Exceptions;
using EZNEW.Development.Entity;
using EZNEW.Development.Query;
using EZNEW.Development.Query.Translation;
using EZNEW.Development.Command;
using EZNEW.Data.Configuration;
using EZNEW.Data.Modification;

namespace EZNEW.Data.Oracle
{
    /// <summary>
    /// Defines database provider implementation for oracle
    /// </summary>
    public class OracleProvider : IDatabaseProvider
    {
        const DatabaseServerType CurrentDatabaseServerType = OracleManager.CurrentDatabaseServerType;

        #region Execute

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            IEnumerable<ICommand> cmdCollection = commands;
            return await ExecuteAsync(server, executionOptions, cmdCollection).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return affected data number</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            #region group execution commands

            IQueryTranslator translator = OracleManager.GetQueryTranslator(DataAccessContext.Create(server));
            List<DatabaseExecutionCommand> databaseExecutionCommands = new List<DatabaseExecutionCommand>();
            var batchExecutionConfig = DataManager.GetBatchExecutionConfiguration(server.ServerType) ?? BatchExecutionConfiguration.Default;
            var groupStatementsCount = batchExecutionConfig.GroupStatementsCount;
            groupStatementsCount = groupStatementsCount < 0 ? 1 : groupStatementsCount;
            var groupParameterCount = batchExecutionConfig.GroupParametersCount;
            groupParameterCount = groupParameterCount < 0 ? 1 : groupParameterCount;
            StringBuilder commandTextBuilder = new StringBuilder();
            CommandParameters parameters = null;
            int statementsCount = 0;
            bool forceReturnValue = false;
            int cmdCount = 0;

            DatabaseExecutionCommand GetGroupExecuteCommand()
            {
                var executionCommand = new DatabaseExecutionCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    MustAffectedData = forceReturnValue,
                    Parameters = parameters
                };
                statementsCount = 0;
                translator.ParameterSequence = 0;
                commandTextBuilder.Clear();
                parameters = null;
                forceReturnValue = false;
                return executionCommand;
            }

            foreach (var command in commands)
            {
                DatabaseExecutionCommand databaseExecutionCommand = GetDatabaseExecutionCommand(translator, command as DefaultCommand);
                if (databaseExecutionCommand == null)
                {
                    continue;
                }

                //Trace log
                OracleManager.LogExecutionCommand(databaseExecutionCommand);

                cmdCount++;
                if (databaseExecutionCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        databaseExecutionCommands.Add(GetGroupExecuteCommand());
                    }
                    databaseExecutionCommands.Add(databaseExecutionCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(databaseExecutionCommand.CommandText);
                parameters = parameters == null ? databaseExecutionCommand.Parameters : parameters.Union(databaseExecutionCommand.Parameters);
                forceReturnValue |= databaseExecutionCommand.MustAffectedData;
                statementsCount++;
                if (translator.ParameterSequence >= groupParameterCount || statementsCount >= groupStatementsCount)
                {
                    databaseExecutionCommands.Add(GetGroupExecuteCommand());
                }
            }
            if (statementsCount > 0)
            {
                databaseExecutionCommands.Add(GetGroupExecuteCommand());
            }

            #endregion

            return await ExecuteDatabaseCommandAsync(server, executionOptions, databaseExecutionCommands, executionOptions?.ExecutionByTransaction ?? cmdCount > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute database command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="databaseExecutionCommands">Database execution commands</param>
        /// <param name="useTransaction">Whether use transaction</param>
        /// <returns>Return affected data number</returns>
        async Task<int> ExecuteDatabaseCommandAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<DatabaseExecutionCommand> databaseExecutionCommands, bool useTransaction)
        {
            int resultValue = 0;
            bool success = true;
            using (var conn = OracleManager.GetConnection(server))
            {
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = OracleManager.GetExecutionTransaction(conn, executionOptions);
                }
                try
                {
                    foreach (var command in databaseExecutionCommands)
                    {
                        var cmdDefinition = new CommandDefinition(command.CommandText, OracleManager.ConvertCmdParameters(command.Parameters), transaction: transaction, commandType: command.CommandType, cancellationToken: executionOptions?.CancellationToken ?? default);
                        var executionResultValue = await conn.ExecuteAsync(cmdDefinition).ConfigureAwait(false);
                        success = success && (!command.MustAffectedData || executionResultValue > 0);
                        resultValue += executionResultValue;
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
        /// Get database execution command
        /// </summary>
        /// <param name="command">Command</param>
        /// <returns>Return database execution command</returns>
        DatabaseExecutionCommand GetDatabaseExecutionCommand(IQueryTranslator queryTranslator, DefaultCommand command)
        {
            DatabaseExecutionCommand GetTextCommand()
            {
                return new DatabaseExecutionCommand()
                {
                    CommandText = command.Text,
                    Parameters = OracleManager.ConvertParameter(command.Parameters),
                    CommandType = OracleManager.GetCommandType(command),
                    MustAffectedData = command.MustAffectedData,
                    HasPreScript = true
                };
            }
            if (command.ExecutionMode == CommandExecutionMode.CommandText)
            {
                return GetTextCommand();
            }
            DatabaseExecutionCommand databaseExecutionCommand = null;
            switch (command.OperationType)
            {
                case CommandOperationType.Insert:
                    databaseExecutionCommand = GetDatabaseInsertionCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Update:
                    databaseExecutionCommand = GetDatabaseUpdateCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Delete:
                    databaseExecutionCommand = GetDatabaseDeletionCommand(queryTranslator, command);
                    break;
                default:
                    databaseExecutionCommand = GetTextCommand();
                    break;
            }
            databaseExecutionCommand.HasPreScript = true;
            return databaseExecutionCommand;
        }

        /// <summary>
        /// Get database insertion execution command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database insertion command</returns>
        DatabaseExecutionCommand GetDatabaseInsertionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = DataManager.GetEditFields(CurrentDatabaseServerType, command.EntityType);
            int fieldCount = fields.GetCount();
            var insertFormatResult = OracleManager.FormatInsertionFields(command.EntityType, fieldCount, fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO {OracleManager.FormatTableName(objectName)} ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)})";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fieldCount;
            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = OracleManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters
            };
        }

        /// <summary>
        /// Get database update command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database update command</returns>
        DatabaseExecutionCommand GetDatabaseUpdateCommand(IQueryTranslator translator, DefaultCommand command)
        {
            if (command?.Fields.IsNullOrEmpty() ?? true)
            {
                throw new EZNEWException($"No fields are set to update");
            }

            #region query translation

            translator.DataAccessContext.SetCommand(command);
            var queryTranslationResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString))
            {
                conditionString += "WHERE " + queryTranslationResult.ConditionString;
            }
            string preScript = queryTranslationResult.PreScript;
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;

            #endregion

            #region script

            CommandParameters parameters = OracleManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = OracleManager.GetFields(command.EntityType, command.Fields);
            int parameterSequence = translator.ParameterSequence;
            List<string> updateSetArray = new List<string>();
            foreach (var field in fields)
            {
                var parameterValue = parameters.GetParameterValue(field.PropertyName);
                var parameterName = field.PropertyName;
                string newValueExpression = string.Empty;
                string fieldName = OracleManager.FormatFieldName(field.FieldName);
                if (parameterValue != null)
                {
                    parameterSequence++;
                    parameterName = OracleManager.FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModificationValue)
                    {
                        var modificationValue = parameterValue as IModificationValue;
                        parameters.ModifyValue(parameterName, modificationValue.Value);
                        if (parameterValue is CalculationModificationValue)
                        {
                            var calculateModificationValue = parameterValue as CalculationModificationValue;
                            string calChar = OracleManager.GetSystemCalculationOperator(calculateModificationValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.{fieldName}{calChar}{OracleManager.ParameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{OracleManager.ParameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{translator.ObjectPetName}.{fieldName}={newValueExpression}");
            }

            string formatedObjectName = OracleManager.FormatTableName(objectName);
            string cmdText;
            if (string.IsNullOrWhiteSpace(joinScript))
            {
                cmdText = $"{preScript}UPDATE {formatedObjectName} {translator.ObjectPetName} SET {string.Join(",", updateSetArray)} {conditionString}";
            }
            else
            {
                string updateTableShortName = "UTB";
                var primaryKeyFormatedResult = FormatWrapJoinPrimaryKeys(command.EntityType, translator.ObjectPetName, translator.ObjectPetName, updateTableShortName);
                cmdText = $"{preScript}MERGE INTO {formatedObjectName} {translator.ObjectPetName} USING (SELECT {string.Join(",", primaryKeyFormatedResult.Item1)} FROM {formatedObjectName} {translator.ObjectPetName} {joinScript} {conditionString}) {updateTableShortName} ON ({string.Join(" AND ", primaryKeyFormatedResult.Item2)}) WHEN MATCHED THEN UPDATE SET {string.Join(",", updateSetArray)}";
            }
            translator.ParameterSequence = parameterSequence;

            #endregion

            #region parameter

            var queryParameters = OracleManager.ConvertParameter(queryTranslationResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = OracleManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// Get database deletion command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database deletion command</returns>
        DatabaseExecutionCommand GetDatabaseDeletionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);

            #region query translation

            var queryTranslationResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString))
            {
                conditionString += "WHERE " + queryTranslationResult.ConditionString;
            }
            string preScript = queryTranslationResult.PreScript;
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            string cmdText = string.Empty;
            string formatedObjName = OracleManager.FormatTableName(objectName);
            if (string.IsNullOrWhiteSpace(joinScript))
            {
                cmdText = $"{preScript}DELETE {formatedObjName} {translator.ObjectPetName} {conditionString}";
            }
            else
            {
                var primaryKeyFields = DataManager.GetFields(CurrentDatabaseServerType, command.EntityType, EntityManager.GetPrimaryKeys(command.EntityType)).ToList();
                if (primaryKeyFields.IsNullOrEmpty())
                {
                    throw new EZNEWException($"{command.EntityType?.FullName} not set primary key");
                }
                string deleteTableShortName = "DTB";
                cmdText = $"{preScript}DELETE FROM {formatedObjName} {deleteTableShortName} WHERE ({string.Join(",", primaryKeyFields.Select(pk => deleteTableShortName + "." + OracleManager.FormatFieldName(pk.FieldName)))}) IN (SELECT {string.Join(",", primaryKeyFields.Select(pk => translator.ObjectPetName + "." + OracleManager.FormatFieldName(pk.FieldName)))} FROM {formatedObjName} {translator.ObjectPetName} {joinScript} {conditionString})";
            }

            #endregion

            #region parameter

            CommandParameters parameters = OracleManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            var queryParameters = OracleManager.ConvertParameter(queryTranslationResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = OracleManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        Tuple<IEnumerable<string>, IEnumerable<string>> FormatWrapJoinPrimaryKeys(Type entityType, string translatorObjectPetName, string sourceObjectPetName, string targetObjectPetName)
        {
            var primaryKeyFields = DataManager.GetFields(CurrentDatabaseServerType, entityType, EntityManager.GetPrimaryKeys(entityType));
            if (primaryKeyFields.IsNullOrEmpty())
            {
                throw new EZNEWException($"{entityType?.FullName} not set primary key");
            }
            return FormatWrapJoinFields(primaryKeyFields, translatorObjectPetName, sourceObjectPetName, targetObjectPetName);
        }

        Tuple<IEnumerable<string>, IEnumerable<string>> FormatWrapJoinFields(IEnumerable<EntityField> fields, string translatorObjectPetName, string sourceObjectPetName, string targetObjectPetName)
        {
            var joinItems = fields.Select(field =>
            {
                string fieldName = OracleManager.FormatFieldName(field.FieldName);
                return $"{sourceObjectPetName}.{fieldName} = {targetObjectPetName}.{fieldName}";
            });
            var queryItems = fields.Select(field =>
            {
                string fieldName = OracleManager.FormatFieldName(field.FieldName);
                return $"{translatorObjectPetName}.{fieldName}";
            });
            return new Tuple<IEnumerable<string>, IEnumerable<string>>(queryItems, joinItems);
        }

        #endregion

        #region Query

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the datas</returns>
        public IEnumerable<T> Query<T>(DatabaseServer server, ICommand command)
        {
            return QueryAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the datas</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = OracleManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var queryTranslationResult = translator.Translate(command.Query);
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = queryTranslationResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    bool hasSort = !string.IsNullOrWhiteSpace(queryTranslationResult.SortString);
                    bool hasCombine = !string.IsNullOrWhiteSpace(queryTranslationResult.CombineScript);
                    bool hasLimit = command.Query.QuerySize > 0;
                    var sortString = hasSort ? $"ORDER BY {queryTranslationResult.SortString}" : string.Empty;
                    string limitCondition = hasLimit ? hasCombine || hasSort ? $"WHERE ROWNUM <= {command.Query.QuerySize}" : $"ROWNUM <= {command.Query.QuerySize}" : string.Empty;
                    var conditionString = OracleManager.CombineLimitCondition(queryTranslationResult.ConditionString, hasCombine || (hasSort && hasLimit) ? string.Empty : limitCondition);
                    var queryFields = OracleManager.GetQueryFields(command.Query, command.EntityType, true);
                    var innerFormatedField = string.Join(",", OracleManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                    var outputFormatedField = string.Join(",", OracleManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    objectName = OracleManager.FormatTableName(objectName);
                    if (hasCombine)
                    {
                        cmdText = hasSort && hasLimit
                            ? $"{queryTranslationResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM (SELECT {innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {queryTranslationResult.CombineScript}) {translator.ObjectPetName} {sortString}) {translator.ObjectPetName} {limitCondition}"
                            : $"{queryTranslationResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {queryTranslationResult.CombineScript}) {translator.ObjectPetName} {sortString} {limitCondition}";
                    }
                    else
                    {
                        cmdText = hasSort && hasLimit
                            ? $"{queryTranslationResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {sortString}) {translator.ObjectPetName} {limitCondition}"
                            : $"{queryTranslationResult.PreScript}SELECT {outputFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {sortString}";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = OracleManager.ConvertCmdParameters(OracleManager.ConvertParameter(queryTranslationResult.Parameters));

            #endregion

            //Trace log
            OracleManager.LogScript(cmdText, queryTranslationResult.Parameters);

            using (var conn = OracleManager.GetConnection(server))
            {
                var tran = OracleManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: OracleManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query paging data
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return paging data</returns>
        public IEnumerable<T> QueryPaging<T>(DatabaseServer server, ICommand command)
        {
            return QueryPagingAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query paging data
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return paging data</returns>
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
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return the datas</returns>
        public IEnumerable<T> QueryOffset<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            return QueryOffsetAsync<T>(server, command, offsetNum, size).Result;
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return the datas</returns>
        public async Task<IEnumerable<T>> QueryOffsetAsync<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = OracleManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    string defaultFieldName = OracleManager.FormatFieldName(OracleManager.GetDefaultFieldName(command.EntityType));
                    int beginRow = offsetNum+1;
                    string conditionString = string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $" WHERE {tranResult.ConditionString}";
                    string offsetConditionString = $"WHERE EZNEW_ROWNUMBER BETWEEN {beginRow} AND {beginRow + size-1}";
                    string sortString = string.IsNullOrWhiteSpace(tranResult.SortString) ? $" ORDER BY {translator.ObjectPetName}.{defaultFieldName} DESC" : $" ORDER BY {tranResult.SortString}";
                    string totalCountAndRowNumber = $"SELECT COUNT({translator.ObjectPetName}.{defaultFieldName}) OVER() AS {DataManager.PagingTotalCountFieldName},ROW_NUMBER() OVER({sortString}) AS EZNEW_ROWNUMBER";
                    var queryFields = OracleManager.GetQueryFields(command.Query, command.EntityType, true);
                    var innerFormatedField = string.Join(",", OracleManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                    var outputFormatedField = string.Join(",", OracleManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    objectName = OracleManager.FormatTableName(objectName);
                    cmdText = string.IsNullOrWhiteSpace(tranResult.CombineScript)
                        ? $"{tranResult.PreScript}SELECT {outputFormatedField},{DataManager.PagingTotalCountFieldName} FROM ({totalCountAndRowNumber},{innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString}) {translator.ObjectPetName} {offsetConditionString}"
                        : $"{tranResult.PreScript}SELECT {outputFormatedField},{DataManager.PagingTotalCountFieldName} FROM ({totalCountAndRowNumber},{innerFormatedField} FROM (SELECT {innerFormatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript}) {translator.ObjectPetName}) {translator.ObjectPetName} {offsetConditionString}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = OracleManager.ConvertCmdParameters(OracleManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            OracleManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = OracleManager.GetConnection(server))
            {
                var tran = OracleManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: OracleManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Indicates whether exists data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Exists data</returns>
        public bool Exists(DatabaseServer server, ICommand command)
        {
            return ExistsAsync(server, command).Result;
        }

        /// <summary>
        /// Indicates whether exists data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Exists data</returns>
        public async Task<bool> ExistsAsync(DatabaseServer server, ICommand command)
        {
            #region query translation

            var translator = OracleManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            command.Query.ClearQueryFields();
            var queryFields = EntityManager.GetPrimaryKeys(command.EntityType).ToArray();
            if (queryFields.IsNullOrEmpty())
            {
                queryFields = EntityManager.GetQueryFields(command.EntityType).ToArray();
            }
            command.Query.AddQueryFields(queryFields);
            var queryTranslationResult = translator.Translate(command.Query);
            string conditionString = string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString) ? string.Empty : $"WHERE {queryTranslationResult.ConditionString}";
            string preScript = queryTranslationResult.PreScript;
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = OracleManager.FormatTableName(translator.DataAccessContext.GetCommandEntityObjectName(command));
            string formatedField = string.Join(",", OracleManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false));
            string cmdText = $"{preScript}SELECT CASE WHEN EXISTS(SELECT {formatedField} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {queryTranslationResult.CombineScript}) THEN 1 ELSE 0 END FROM DUAL";

            #endregion

            #region parameter

            var parameters = OracleManager.ConvertCmdParameters(OracleManager.ConvertParameter(queryTranslationResult.Parameters));

            #endregion

            //Trace log
            OracleManager.LogScript(cmdText, queryTranslationResult.Parameters);

            using (var conn = OracleManager.GetConnection(server))
            {
                var tran = OracleManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, cancellationToken: command.Query?.GetCancellationToken() ?? default);
                int value = await conn.ExecuteScalarAsync<int>(cmdDefinition).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public T AggregateValue<T>(DatabaseServer server, ICommand command)
        {
            return AggregateValueAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public async Task<T> AggregateValueAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            bool queryObject = command.Query.ExecutionMode == QueryExecutionMode.QueryObject;
            string funcName = OracleManager.GetAggregationFunctionName(command.OperationType);
            EntityField defaultField = null;
            if (queryObject)
            {
                if (string.IsNullOrWhiteSpace(funcName))
                {
                    throw new NotSupportedException($"Not support {command.OperationType}");
                }
                if (OracleManager.CheckAggregationOperationMustNeedField(command.OperationType))
                {
                    if (command.Query.QueryFields.IsNullOrEmpty())
                    {
                        throw new EZNEWException($"Must specify the field to perform for the {funcName} operation");
                    }
                    defaultField = DataManager.GetField(CurrentDatabaseServerType, command.EntityType, command.Query.QueryFields.First());
                }
                else
                {
                    defaultField = DataManager.GetDefaultField(CurrentDatabaseServerType, command.EntityType);
                }

                //combine fields
                if (!command.Query.Combines.IsNullOrEmpty())
                {
                    var combineKeys = EntityManager.GetPrimaryKeys(command.EntityType).Union(new string[1] { defaultField.PropertyName }).ToArray();
                    command.Query.ClearQueryFields();
                    foreach (var combineEntry in command.Query.Combines)
                    {
                        combineEntry.Query.ClearQueryFields();
                        if (combineKeys.IsNullOrEmpty())
                        {
                            combineEntry.Query.ClearNotQueryFields();
                            command.Query.ClearNotQueryFields();
                        }
                        else
                        {
                            combineEntry.Query.AddQueryFields(combineKeys);
                            command.Query.AddQueryFields(combineKeys);
                        }
                    }
                }
            }
            IQueryTranslator translator = OracleManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var queryTranslationResult = translator.Translate(command.Query);
            string joinScript = queryTranslationResult.AllowJoin ? queryTranslationResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = queryTranslationResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    var conditionString = string.IsNullOrWhiteSpace(queryTranslationResult.ConditionString) ? string.Empty : $"WHERE {queryTranslationResult.ConditionString}";
                    var defaultQueryField = OracleManager.FormatField(translator.ObjectPetName, defaultField, false);
                    objectName = OracleManager.FormatTableName(objectName);
                    cmdText = string.IsNullOrWhiteSpace(queryTranslationResult.CombineScript)
                        ? $"{queryTranslationResult.PreScript}SELECT {funcName}({defaultQueryField}) FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString}"
                        : $"{queryTranslationResult.PreScript}SELECT {funcName}({defaultQueryField}) FROM (SELECT {string.Join(",", OracleManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {objectName} {translator.ObjectPetName} {joinScript} {conditionString} {queryTranslationResult.CombineScript}) {translator.ObjectPetName}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = OracleManager.ConvertCmdParameters(OracleManager.ConvertParameter(queryTranslationResult.Parameters));

            #endregion

            //Trace log
            OracleManager.LogScript(cmdText, queryTranslationResult.Parameters);

            using (var conn = OracleManager.GetConnection(server))
            {
                var tran = OracleManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: OracleManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.ExecuteScalarAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data set
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Query command</param>
        /// <returns>Return data set</returns>
        public async Task<DataSet> QueryMultipleAsync(DatabaseServer server, ICommand command)
        {
            //Trace log
            OracleManager.LogScript(command.Text, command.Parameters);
            using (var conn = OracleManager.GetConnection(server))
            {
                var tran = OracleManager.GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = OracleManager.ConvertCmdParameters(OracleManager.ConvertParameter(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.Text, parameters, transaction: tran, commandType: OracleManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
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

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertionOptions">Insertion options</param>
        public void BulkInsert(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertionOptions = null)
        {
            BulkInsertAsync(server, dataTable).Wait();
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public async Task BulkInsertAsync(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertOptions = null)
        {
            if (server == null)
            {
                throw new ArgumentNullException(nameof(server));
            }
            if (dataTable == null)
            {
                throw new ArgumentNullException(nameof(dataTable));
            }
            OracleBulkInsertionOptions oracleBulkInsertOptions = bulkInsertOptions as OracleBulkInsertionOptions;
            oracleBulkInsertOptions = oracleBulkInsertOptions ?? new OracleBulkInsertionOptions();
            using (OracleBulkCopy oracleBulkCopy = new OracleBulkCopy(server?.ConnectionString))
            {
                try
                {
                    oracleBulkCopy.DestinationTableName = dataTable.TableName;
                    if (oracleBulkInsertOptions.UseTransaction)
                    {
                        oracleBulkCopy.BulkCopyOptions = OracleBulkCopyOptions.UseInternalTransaction;
                    }
                    if (!oracleBulkInsertOptions.ColumnMappings.IsNullOrEmpty())
                    {
                        oracleBulkInsertOptions.ColumnMappings.ForEach(c =>
                        {
                            if (oracleBulkInsertOptions.Uppercase)
                            {
                                c.DestinationColumn = c.DestinationColumn.ToUpper();
                            }
                            if (oracleBulkInsertOptions.WrapWithQuotes)
                            {
                                c.DestinationColumn = OracleManager.WrapKeyword(c.DestinationColumn);
                            }
                            oracleBulkCopy.ColumnMappings.Add(c);
                        });
                    }
                    else
                    {
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            string destName = column.ColumnName;
                            if (oracleBulkInsertOptions.Uppercase)
                            {
                                destName = destName.ToUpper();
                            }
                            if (oracleBulkInsertOptions.WrapWithQuotes)
                            {
                                destName = OracleManager.WrapKeyword(destName);
                            }
                            oracleBulkCopy.ColumnMappings.Add(new OracleBulkCopyColumnMapping()
                            {
                                SourceColumn = column.ColumnName,
                                DestinationColumn = destName
                            });
                        }
                    }
                    if (oracleBulkInsertOptions.BulkCopyTimeout > 0)
                    {
                        oracleBulkCopy.BulkCopyTimeout = oracleBulkInsertOptions.BulkCopyTimeout;
                    }
                    if (oracleBulkInsertOptions.BatchSize > 0)
                    {
                        oracleBulkCopy.BatchSize = oracleBulkInsertOptions.BatchSize;
                    }
                    if (oracleBulkInsertOptions.NotifyAfter > 0)
                    {
                        oracleBulkCopy.NotifyAfter = oracleBulkInsertOptions.NotifyAfter;
                    }
                    if (oracleBulkInsertOptions.Uppercase)
                    {
                        oracleBulkCopy.DestinationTableName = oracleBulkCopy.DestinationTableName.ToUpper();
                    }
                    if (oracleBulkInsertOptions.WrapWithQuotes)
                    {
                        oracleBulkCopy.DestinationTableName = OracleManager.WrapKeyword(oracleBulkCopy.DestinationTableName);
                    }
                    oracleBulkCopy.WriteToServer(dataTable);
                    await Task.CompletedTask;
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    if (oracleBulkCopy?.Connection != null && oracleBulkCopy.Connection.State != ConnectionState.Closed)
                    {
                        oracleBulkCopy.Connection.Close();
                    }
                }
            }
        }

        #endregion
    }
}
